from __future__ import annotations

import hashlib
import inspect
import os
import posixpath
import stat
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from asyncssh import SFTPAttrs

from hpc_mcp.project.manifest import (
    FileEntry,
    ProjectManifest,
    SyncPlan,
    _fixed_directory,
    _ignore_spec,
    _is_ignored,
)
from hpc_mcp.project.paths import LocalPathGuard, RemotePathGuard
from hpc_mcp.project.registry import ProjectRecord
from hpc_mcp.remote.names import directory_entry_name
from hpc_mcp.ssh.protocol import SshClientProtocol


_CHUNK_SIZE = 1024 * 1024
_MANIFEST_RELATIVE_PATH = ".hpc/manifest.json"


@dataclass(frozen=True, slots=True)
class SyncResult:
    uploaded: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    manifest_updated: bool = False


class ProjectSyncService:
    def __init__(self, client: SshClientProtocol) -> None:
        self._client = client

    async def plan(self, record: ProjectRecord) -> SyncPlan:
        local = ProjectManifest.scan(record.local_root)
        async with self._client.sftp() as sftp:
            remote_root = await RemotePathGuard.resolve(self._client, record, ".")
            previous = await _read_previous_manifest(sftp, remote_root)
            remote = await _scan_remote_manifest(
                sftp,
                remote_root,
                record.local_root,
            )
        return previous.diff(local=local, remote=remote)

    async def execute(
        self,
        record: ProjectRecord,
        plan: SyncPlan | None = None,
    ) -> SyncResult:
        if plan is None:
            plan = await self.plan(record)

        planned_local = plan.local_manifest
        current_local = ProjectManifest.scan(record.local_root)
        if planned_local is None:
            return SyncResult(
                failed=sorted(set(plan.upload)),
                conflicts=sorted(set(plan.conflicts)),
            )
        if current_local != planned_local:
            return SyncResult(
                failed=_changed_manifest_paths(planned_local, current_local),
                conflicts=sorted(set(plan.conflicts)),
            )

        uploaded: list[str] = []
        failed: list[str] = []
        async with self._client.sftp() as sftp:
            remote_root = await RemotePathGuard.resolve(self._client, record, ".")
            previous = await _read_previous_manifest(sftp, remote_root)
            remote = await _scan_remote_manifest(
                sftp,
                remote_root,
                record.local_root,
            )
            refreshed = previous.diff(local=planned_local, remote=remote)
            overlap = set(plan.upload) & set(plan.conflicts)
            conflicts = sorted(set(refreshed.conflicts) | overlap)
            conflict_set = set(conflicts)
            uploads = [path for path in refreshed.upload if path not in conflict_set]

            for relative_path in uploads:
                try:
                    expected = planned_local.files.get(relative_path)
                    if expected is None:
                        raise ValueError("upload path is not a scanned regular file")
                    source = LocalPathGuard.resolve(record, relative_path)
                    target = await RemotePathGuard.resolve_creation(
                        self._client,
                        record,
                        relative_path,
                    )
                    await _reject_remote_symlink_components(
                        sftp,
                        remote_root,
                        relative_path,
                    )
                    remote_entry = await _read_remote_file_entry(sftp, target)
                    if remote_entry != remote.files.get(relative_path):
                        revalidated_files = dict(remote.files)
                        if remote_entry is None:
                            revalidated_files.pop(relative_path, None)
                        else:
                            revalidated_files[relative_path] = remote_entry
                        revalidated = previous.diff(
                            local=planned_local,
                            remote=ProjectManifest(files=revalidated_files),
                        )
                        if relative_path in revalidated.conflicts:
                            conflicts.append(relative_path)
                            conflict_set.add(relative_path)
                        else:
                            failed.append(relative_path)
                        continue
                    await _atomic_upload(sftp, source, target, expected)
                except Exception:
                    failed.append(relative_path)
                else:
                    uploaded.append(relative_path)

            manifest_updated = False
            if not failed and not conflicts:
                try:
                    manifest_target = await RemotePathGuard.resolve_creation(
                        self._client,
                        record,
                        _MANIFEST_RELATIVE_PATH,
                    )
                    await _reject_remote_symlink_components(
                        sftp,
                        remote_root,
                        _MANIFEST_RELATIVE_PATH,
                    )
                    await _atomic_write_bytes(
                        sftp,
                        planned_local.to_json().encode("utf-8"),
                        manifest_target,
                    )
                except Exception:
                    failed.append(_MANIFEST_RELATIVE_PATH)
                else:
                    manifest_updated = True

        return SyncResult(
            uploaded=uploaded,
            failed=failed,
            conflicts=conflicts,
            manifest_updated=manifest_updated,
        )


def _changed_manifest_paths(
    planned: ProjectManifest,
    current: ProjectManifest,
) -> list[str]:
    return sorted(
        path
        for path in set(planned.files) | set(current.files)
        if planned.files.get(path) != current.files.get(path)
    )


async def _read_previous_manifest(sftp: Any, remote_root: str) -> ProjectManifest:
    path = posixpath.join(remote_root, _MANIFEST_RELATIVE_PATH)
    if not await _exists(sftp, path):
        return ProjectManifest()
    handle = await _maybe_await(sftp.open(path, "rb"))
    try:
        data = await _maybe_await(handle.read())
    finally:
        await _maybe_await(handle.close())
    return ProjectManifest.from_json(data)


async def _scan_remote_manifest(
    sftp: Any,
    remote_root: str,
    local_root: Path,
) -> ProjectManifest:
    if not await _exists(sftp, remote_root):
        return ProjectManifest()

    ignore_spec = _ignore_spec(local_root)
    files: dict[str, FileEntry] = {}

    async def visit(directory: str, relative_directory: str = "") -> None:
        async for name in _scandir(sftp, directory):
            filename = directory_entry_name(name.filename)
            if filename is None:
                continue
            relative = posixpath.join(relative_directory, filename)
            if relative == _MANIFEST_RELATIVE_PATH:
                continue
            attrs = name.attrs
            permissions = attrs.permissions
            if permissions is None:
                attrs = await _maybe_await(
                    sftp.lstat(posixpath.join(directory, filename))
                )
                permissions = attrs.permissions
            if permissions is None or stat.S_ISLNK(permissions):
                continue
            if stat.S_ISDIR(permissions):
                if _fixed_directory(relative):
                    continue
                if _is_ignored(relative, ignore_spec, is_directory=True):
                    continue
                await visit(posixpath.join(directory, filename), relative)
            elif stat.S_ISREG(permissions):
                if _is_ignored(relative, ignore_spec):
                    continue
                files[relative] = await _hash_remote_file(
                    sftp,
                    posixpath.join(directory, filename),
                    attrs,
                )

    await visit(remote_root)
    return ProjectManifest(files=dict(sorted(files.items())))


async def _hash_remote_file(sftp: Any, path: str, attrs: Any) -> FileEntry:
    digest = hashlib.sha256()
    size = 0
    handle = await _maybe_await(sftp.open(path, "rb"))
    try:
        while chunk := await _maybe_await(handle.read(_CHUNK_SIZE)):
            digest.update(chunk)
            size += len(chunk)
    finally:
        await _maybe_await(handle.close())
    seconds = int(getattr(attrs, "mtime", 0) or 0)
    nanoseconds = int(getattr(attrs, "mtime_ns", 0) or 0)
    return FileEntry(
        size=size,
        mtime_ns=seconds * 1_000_000_000 + nanoseconds,
        sha256=digest.hexdigest(),
    )


async def _read_remote_file_entry(sftp: Any, path: str) -> FileEntry | None:
    if not await _exists(sftp, path):
        return None
    attrs = await _maybe_await(sftp.lstat(path))
    permissions = attrs.permissions
    if permissions is None or not stat.S_ISREG(permissions):
        raise ValueError("remote upload target is not a regular file")
    return await _hash_remote_file(sftp, path, attrs)


async def _reject_remote_symlink_components(
    sftp: Any,
    remote_root: str,
    relative_path: str,
) -> None:
    requested = PurePosixPath(relative_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise ValueError("upload path is outside the remote project root")

    current = remote_root
    for part in requested.parts[:-1]:
        current = posixpath.join(current, part)
        lexists = getattr(sftp, "lexists", None)
        if lexists is not None and not await _maybe_await(lexists(current)):
            continue
        attrs = await _maybe_await(sftp.lstat(current))
        permissions = attrs.permissions
        if permissions is None or stat.S_ISLNK(permissions):
            raise ValueError("remote upload path contains a symbolic link")


async def _atomic_upload(
    sftp: Any,
    source: Path,
    target: str,
    expected: FileEntry,
) -> None:
    await _maybe_await(sftp.makedirs(posixpath.dirname(target), exist_ok=True))
    temporary = f"{target}.hpc-upload-{uuid4().hex}"
    descriptor: int | None = None
    handle: Any | None = None
    created = False
    try:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(source, flags)
        details = os.fstat(descriptor)
        if not stat.S_ISREG(details.st_mode):
            raise ValueError("upload source is not a regular file")
        if details.st_size != expected.size or details.st_mtime_ns != expected.mtime_ns:
            raise ValueError("upload source changed after planning")
        handle = await _maybe_await(
            sftp.open(temporary, "xb", attrs=SFTPAttrs(permissions=0o600))
        )
        created = True
        digest = hashlib.sha256()
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(_CHUNK_SIZE):
                digest.update(chunk)
                await _maybe_await(handle.write(chunk))
        if digest.hexdigest() != expected.sha256:
            raise ValueError("upload source changed after planning")
        # AsyncSSH writes are awaited directly; only buffered adapters expose flush.
        flush = getattr(handle, "flush", None)
        if flush is not None:
            await _maybe_await(flush())
        await _maybe_await(handle.close())
        handle = None
        await _rename_over(sftp, temporary, target)
    except BaseException:
        if handle is not None:
            try:
                await _maybe_await(handle.close())
            except Exception:
                pass
        if created:
            try:
                await _maybe_await(sftp.remove(temporary))
            except Exception:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


async def _atomic_write_bytes(sftp: Any, data: bytes, target: str) -> None:
    await _maybe_await(sftp.makedirs(posixpath.dirname(target), exist_ok=True))
    temporary = f"{target}.hpc-upload-{uuid4().hex}"
    handle: Any | None = None
    created = False
    try:
        handle = await _maybe_await(
            sftp.open(temporary, "xb", attrs=SFTPAttrs(permissions=0o600))
        )
        created = True
        for offset in range(0, len(data), _CHUNK_SIZE):
            await _maybe_await(handle.write(data[offset : offset + _CHUNK_SIZE]))
        flush = getattr(handle, "flush", None)
        if flush is not None:
            await _maybe_await(flush())
        await _maybe_await(handle.close())
        handle = None
        await _rename_over(sftp, temporary, target)
    except BaseException:
        if handle is not None:
            try:
                await _maybe_await(handle.close())
            except Exception:
                pass
        if created:
            try:
                await _maybe_await(sftp.remove(temporary))
            except Exception:
                pass
        raise


async def _rename_over(sftp: Any, source: str, target: str) -> None:
    posix_rename = getattr(sftp, "posix_rename", None)
    if posix_rename is not None:
        await _maybe_await(posix_rename(source, target))
    else:
        await _maybe_await(sftp.rename(source, target))


async def _exists(sftp: Any, path: str) -> bool:
    exists = getattr(sftp, "exists", None)
    if exists is not None:
        return bool(await _maybe_await(exists(path)))
    try:
        await _maybe_await(sftp.stat(path))
    except (FileNotFoundError, OSError):
        return False
    return True


async def _scandir(sftp: Any, path: str) -> AsyncIterator[Any]:
    entries = sftp.scandir(path)
    if inspect.isawaitable(entries):
        entries = await entries
    if hasattr(entries, "__aiter__"):
        async for entry in entries:
            yield entry
    else:
        for entry in entries:
            yield entry


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
