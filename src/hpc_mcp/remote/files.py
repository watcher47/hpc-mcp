from __future__ import annotations

import hashlib
import inspect
import json
import os
import posixpath
import stat as stat_module
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from functools import wraps
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

from asyncssh import SFTPAttrs, SFTPError

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import LocalPathGuard, RemotePathGuard
from hpc_mcp.project.registry import ProjectRecord
from hpc_mcp.remote.names import directory_entry_name
from hpc_mcp.ssh.policy import RemoteCommand
from hpc_mcp.ssh.protocol import SshClientProtocol


READ_LIMIT_BYTES = 256 * 1024
SEARCH_MATCH_LIMIT = 500
_TRANSFER_CHUNK_BYTES = 1024 * 1024


def _redact_io_errors(operation: Any) -> Any:
    """Keep filesystem/transport diagnostics out of the public service boundary."""
    @wraps(operation)
    async def guarded(*args: Any, **kwargs: Any) -> Any:
        try:
            return await operation(*args, **kwargs)
        except HpcMcpError as error:
            raise error from None
        except UnicodeError:
            raise HpcMcpError(
                "REMOTE_ENCODING_ERROR", "File operation could not encode or decode text"
            ) from None
        except (OSError, SFTPError, ValueError):
            raise HpcMcpError(
                "REMOTE_IO_ERROR", "File operation could not be completed"
            ) from None
    return guarded


@dataclass(frozen=True, slots=True)
class RemoteEntry:
    name: str
    relative_path: str
    kind: str
    size: int
    mode: int
    mtime_ns: int
    uid: int | None = None
    gid: int | None = None


@dataclass(frozen=True, slots=True)
class RemoteStat:
    relative_path: str
    kind: str
    size: int
    mode: int
    mtime_ns: int
    uid: int | None = None
    gid: int | None = None


@dataclass(frozen=True, slots=True)
class ReadResult:
    relative_path: str
    content: str
    offset: int
    next_offset: int | None
    size: int
    sha256: str
    truncated: bool


@dataclass(frozen=True, slots=True)
class SearchMatch:
    relative_path: str
    line_number: int
    column: int
    text: str


@dataclass(frozen=True, slots=True)
class SearchResult:
    matches: list[SearchMatch] = field(default_factory=list)
    truncated: bool = False


@dataclass(frozen=True, slots=True)
class TransferResult:
    local_relative_path: str
    remote_relative_path: str
    size: int
    sha256: str


class RemoteFileService:
    def __init__(self, registry: Any, client: SshClientProtocol) -> None:
        self._registry = registry
        self._client = client

    @_redact_io_errors
    async def list(
        self,
        project_id: str,
        relative_path: str = ".",
    ) -> list[RemoteEntry]:
        record = await _project_record(self._registry, project_id)
        target = await RemotePathGuard.resolve(self._client, record, relative_path)
        async with self._client.sftp() as sftp:
            attrs = await _maybe_await(sftp.lstat(target))
            if not _is_kind(attrs, stat_module.S_ISDIR):
                raise HpcMcpError("REMOTE_NOT_DIRECTORY", "Remote path is not a directory")
            entries: list[RemoteEntry] = []
            async for item in _scandir(sftp, target):
                name = directory_entry_name(item.filename)
                if name is None:
                    continue
                item_attrs = item.attrs
                if getattr(item_attrs, "permissions", None) is None:
                    item_attrs = await _maybe_await(
                        sftp.lstat(posixpath.join(target, name))
                    )
                child_relative = _join_relative(relative_path, name)
                metadata = _metadata(child_relative, item_attrs)
                entries.append(
                    RemoteEntry(
                        name=name,
                        relative_path=metadata.relative_path,
                        kind=metadata.kind,
                        size=metadata.size,
                        mode=metadata.mode,
                        mtime_ns=metadata.mtime_ns,
                        uid=metadata.uid,
                        gid=metadata.gid,
                    )
                )
        return sorted(entries, key=lambda entry: entry.name)

    @_redact_io_errors
    async def stat(self, project_id: str, relative_path: str) -> RemoteStat:
        record = await _project_record(self._registry, project_id)
        target = await RemotePathGuard.resolve(self._client, record, relative_path)
        async with self._client.sftp() as sftp:
            attrs = await _maybe_await(sftp.lstat(target))
        return _metadata(_normalized_relative(relative_path), attrs)

    @_redact_io_errors
    async def read(
        self,
        project_id: str,
        relative_path: str,
        offset: int = 0,
        limit: int = READ_LIMIT_BYTES,
    ) -> ReadResult:
        if offset < 0:
            raise HpcMcpError("INVALID_OFFSET", "Read offset must be non-negative")
        if limit <= 0:
            raise HpcMcpError("INVALID_LIMIT", "Read limit must be positive")
        retained_limit = min(limit, READ_LIMIT_BYTES)
        record = await _project_record(self._registry, project_id)
        target = await RemotePathGuard.resolve(self._client, record, relative_path)
        digest = hashlib.sha256()
        retained = bytearray()
        total = 0

        async with self._client.sftp() as sftp:
            attrs = await _maybe_await(sftp.lstat(target))
            if not _is_kind(attrs, stat_module.S_ISREG):
                raise HpcMcpError("REMOTE_NOT_FILE", "Remote path is not a regular file")
            handle = await _maybe_await(sftp.open(target, "rb"))
            try:
                while chunk := await _maybe_await(handle.read(_TRANSFER_CHUNK_BYTES)):
                    digest.update(chunk)
                    chunk_start = total
                    total += len(chunk)
                    selection_start = max(offset, chunk_start)
                    selection_end = min(offset + retained_limit, total)
                    if selection_start < selection_end:
                        retained.extend(
                            chunk[
                                selection_start - chunk_start : selection_end - chunk_start
                            ]
                        )
            except BaseException:
                try:
                    await _maybe_await(handle.close())
                except Exception:
                    pass
                raise
            else:
                await _maybe_await(handle.close())

        content_bytes, end_offset = _decode_read_chunk(
            bytes(retained), offset, at_eof=offset + len(retained) >= total
        )
        truncated = end_offset < total
        return ReadResult(
            relative_path=_normalized_relative(relative_path),
            content=content_bytes.decode("utf-8"),
            offset=offset,
            next_offset=end_offset if truncated else None,
            size=total,
            sha256=digest.hexdigest(),
            truncated=truncated,
        )

    @_redact_io_errors
    async def search(
        self,
        project_id: str,
        pattern: str,
        relative_path: str = ".",
        max_matches: int = SEARCH_MATCH_LIMIT,
    ) -> SearchResult:
        if not pattern:
            raise HpcMcpError("INVALID_PATTERN", "Search pattern must not be empty")
        if max_matches <= 0:
            raise HpcMcpError("INVALID_LIMIT", "Search match limit must be positive")
        cap = min(max_matches, SEARCH_MATCH_LIMIT)
        record = await _project_record(self._registry, project_id)
        root = await RemotePathGuard.resolve(self._client, record, ".")
        target = await RemotePathGuard.resolve(self._client, record, relative_path)
        command = RemoteCommand(
            "rg",
            [
                "--json",
                "--max-count",
                str(SEARCH_MATCH_LIMIT),
                "--fixed-strings",
                "--no-messages",
                "--",
                pattern,
                target,
            ],
            root,
        )
        result = await self._client.run(command)
        if result.exit_status not in {0, 1}:
            raise HpcMcpError(
                "REMOTE_SEARCH_FAILED",
                "Remote search failed",
                details={"exit_status": result.exit_status},
            )
        return _parse_rg_matches(result.stdout, root, cap, result.truncated)

    @_redact_io_errors
    async def upload(
        self,
        project_id: str,
        local_relative_path: str,
        remote_relative_path: str | None = None,
    ) -> TransferResult:
        remote_relative_path = remote_relative_path or local_relative_path
        record = await _project_record(self._registry, project_id)
        source = LocalPathGuard.resolve(record, local_relative_path)
        _reject_local_symlink(record, local_relative_path)
        target = await RemotePathGuard.resolve_creation(
            self._client, record, remote_relative_path
        )
        async with self._client.sftp() as sftp:
            remote_root = await RemotePathGuard.resolve(self._client, record, ".")
            await _reject_remote_symlink_components(
                sftp, remote_root, remote_relative_path
            )
            size, digest = await _atomic_upload(sftp, source, target)
        return TransferResult(
            local_relative_path=_normalized_relative(local_relative_path),
            remote_relative_path=_normalized_relative(remote_relative_path),
            size=size,
            sha256=digest,
        )

    @_redact_io_errors
    async def download(
        self,
        project_id: str,
        remote_relative_path: str,
        local_relative_path: str | None = None,
    ) -> TransferResult:
        local_relative_path = local_relative_path or remote_relative_path
        record = await _project_record(self._registry, project_id)
        source = await RemotePathGuard.resolve(
            self._client, record, remote_relative_path
        )
        target = LocalPathGuard.resolve(record, local_relative_path)
        _reject_local_symlink(record, local_relative_path)
        async with self._client.sftp() as sftp:
            attrs = await _maybe_await(sftp.lstat(source))
            if not _is_kind(attrs, stat_module.S_ISREG):
                raise HpcMcpError("REMOTE_NOT_FILE", "Remote path is not a regular file")
            size, digest = await _atomic_download(sftp, source, target, attrs)
        return TransferResult(
            local_relative_path=_normalized_relative(local_relative_path),
            remote_relative_path=_normalized_relative(remote_relative_path),
            size=size,
            sha256=digest,
        )


async def _project_record(registry: Any, project_id: str) -> ProjectRecord:
    lookup = getattr(registry, "get", None)
    if lookup is not None:
        record = await _maybe_await(lookup(project_id))
    elif isinstance(registry, Mapping):
        record = registry.get(project_id)
    elif callable(registry):
        record = await _maybe_await(registry(project_id))
    else:
        record = None
    if record is None:
        raise HpcMcpError(
            "PROJECT_NOT_FOUND",
            "Project is not registered",
            details={"project_id": project_id},
        )
    return record


def _metadata(relative_path: str, attrs: Any) -> RemoteStat:
    permissions = int(getattr(attrs, "permissions", 0) or 0)
    seconds = int(getattr(attrs, "mtime", 0) or 0)
    nanoseconds = int(getattr(attrs, "mtime_ns", 0) or 0)
    return RemoteStat(
        relative_path=relative_path,
        kind=_kind(permissions),
        size=int(getattr(attrs, "size", 0) or 0),
        mode=stat_module.S_IMODE(permissions),
        mtime_ns=seconds * 1_000_000_000 + nanoseconds,
        uid=getattr(attrs, "uid", None),
        gid=getattr(attrs, "gid", None),
    )


def _kind(permissions: int) -> str:
    if stat_module.S_ISREG(permissions):
        return "file"
    if stat_module.S_ISDIR(permissions):
        return "directory"
    if stat_module.S_ISLNK(permissions):
        return "symlink"
    return "other"


def _is_kind(attrs: Any, predicate: Any) -> bool:
    permissions = getattr(attrs, "permissions", None)
    return permissions is not None and predicate(permissions)


def _decode_read_chunk(data: bytes, offset: int, *, at_eof: bool) -> tuple[bytes, int]:
    try:
        data.decode("utf-8")
        return data, offset + len(data)
    except UnicodeDecodeError as error:
        if not at_eof and error.reason == "unexpected end of data" and error.end == len(data):
            complete = data[: error.start]
            try:
                complete.decode("utf-8")
            except UnicodeDecodeError:
                pass
            else:
                if not complete:
                    raise HpcMcpError(
                        "READ_LIMIT_TOO_SMALL", "Read limit cannot hold the next UTF-8 character"
                    ) from None
                return complete, offset + len(complete)
        raise HpcMcpError(
            "REMOTE_DECODE_ERROR",
            "Remote file is not valid UTF-8 at the requested offset",
            details={"offset": offset + error.start},
        ) from None


def _parse_rg_matches(
    stdout: str,
    root: str,
    cap: int,
    output_truncated: bool,
) -> SearchResult:
    matches: list[SearchMatch] = []
    truncated = output_truncated
    per_file_counts: dict[str, int] = {}
    for raw_line in stdout.splitlines():
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        if event.get("type") != "match":
            continue
        data = event.get("data", {})
        path = data.get("path", {}).get("text")
        line = data.get("lines", {}).get("text")
        submatches = data.get("submatches") or []
        if not isinstance(path, str) or not isinstance(line, str):
            continue
        per_file_counts[path] = per_file_counts.get(path, 0) + 1
        if per_file_counts[path] >= SEARCH_MATCH_LIMIT:
            # rg's producer cap may have hidden additional lines in this file.
            truncated = True
        if len(matches) >= cap:
            truncated = True
            break
        matches.append(
            SearchMatch(
                relative_path=_remote_relative(root, path),
                line_number=int(data.get("line_number", 0) or 0),
                column=int(submatches[0].get("start", 0)) + 1 if submatches else 1,
                text=line.rstrip("\r\n"),
            )
        )
    return SearchResult(matches=matches, truncated=truncated)


async def _atomic_upload(sftp: Any, source: Path, target: str) -> tuple[int, str]:
    await _maybe_await(sftp.makedirs(posixpath.dirname(target), exist_ok=True))
    temporary = f"{target}.hpc-upload-{uuid4().hex}"
    descriptor: int | None = None
    handle: Any | None = None
    created = False
    try:
        flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(source, flags)
        source_stat = os.fstat(descriptor)
        if not stat_module.S_ISREG(source_stat.st_mode):
            raise HpcMcpError("LOCAL_NOT_FILE", "Upload source is not a regular file")
        handle = await _maybe_await(sftp.open(temporary, "xb", attrs=SFTPAttrs(permissions=0o600)))
        created = True
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            while chunk := stream.read(_TRANSFER_CHUNK_BYTES):
                digest.update(chunk)
                size += len(chunk)
                await _maybe_await(handle.write(chunk))
        # AsyncSSH writes are awaited directly; only buffered adapters expose flush.
        flush = getattr(handle, "flush", None)
        if flush is not None:
            await _maybe_await(flush())
        await _maybe_await(handle.close())
        handle = None
        await _maybe_await(sftp.chmod(temporary, stat_module.S_IMODE(source_stat.st_mode)))
        await _rename_over(sftp, temporary, target)
        return size, digest.hexdigest()
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


async def _atomic_download(
    sftp: Any,
    source: str,
    target: Path,
    attrs: Any,
) -> tuple[int, str]:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f"{target.name}.hpc-download-{uuid4().hex}")
    descriptor: int | None = None
    handle: Any | None = None
    created = False
    try:
        handle = await _maybe_await(sftp.open(source, "rb"))
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_BINARY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(temporary, flags, 0o600)
        created = True
        digest = hashlib.sha256()
        size = 0
        with os.fdopen(descriptor, "wb", closefd=False) as stream:
            while chunk := await _maybe_await(handle.read(_TRANSFER_CHUNK_BYTES)):
                stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            stream.flush()
            os.fsync(descriptor)
        remote_permissions = int(getattr(attrs, "permissions", 0) or 0)
        mode = stat_module.S_IMODE(remote_permissions)
        if hasattr(os, "fchmod"):
            os.fchmod(descriptor, mode)
        os.close(descriptor)
        descriptor = None
        if not hasattr(os, "fchmod"):
            # Windows supports chmod(path), but not descriptor-based fchmod.
            os.chmod(temporary, mode)
        await _maybe_await(handle.close())
        handle = None
        os.replace(temporary, target)
        return size, digest.hexdigest()
    except BaseException:
        if descriptor is not None:
            os.close(descriptor)
            descriptor = None
        if handle is not None:
            try:
                await _maybe_await(handle.close())
            except Exception:
                pass
        if created:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    finally:
        if descriptor is not None:
            os.close(descriptor)


async def _write_remote_bytes(
    sftp: Any,
    target: str,
    data: bytes,
    mode: int,
    temporary_tag: str = "patch",
) -> None:
    temporary = f"{target}.hpc-{temporary_tag}-{uuid4().hex}"
    handle: Any | None = None
    created = False
    try:
        handle = await _maybe_await(sftp.open(temporary, "xb", attrs=SFTPAttrs(permissions=0o600)))
        created = True
        for offset in range(0, len(data), _TRANSFER_CHUNK_BYTES):
            await _maybe_await(handle.write(data[offset : offset + _TRANSFER_CHUNK_BYTES]))
        flush = getattr(handle, "flush", None)
        if flush is not None:
            await _maybe_await(flush())
        await _maybe_await(handle.close())
        handle = None
        await _maybe_await(sftp.chmod(temporary, mode))
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


async def _read_remote_bytes(sftp: Any, path: str) -> bytes:
    chunks: list[bytes] = []
    handle = await _maybe_await(sftp.open(path, "rb"))
    try:
        while chunk := await _maybe_await(handle.read(_TRANSFER_CHUNK_BYTES)):
            chunks.append(chunk)
    except BaseException:
        try:
            await _maybe_await(handle.close())
        except Exception:
            pass
        raise
    else:
        await _maybe_await(handle.close())
    return b"".join(chunks)


async def _reject_remote_symlink_components(
    sftp: Any,
    remote_root: str,
    relative_path: str,
) -> None:
    requested = PurePosixPath(relative_path)
    if requested.is_absolute() or ".." in requested.parts:
        raise HpcMcpError("PATH_OUTSIDE_PROJECT", "Path resolves outside the project root")
    current = remote_root
    for part in requested.parts[:-1]:
        current = posixpath.join(current, part)
        lexists = getattr(sftp, "lexists", None)
        if lexists is not None and not await _maybe_await(lexists(current)):
            continue
        try:
            attrs = await _maybe_await(sftp.lstat(current))
        except (FileNotFoundError, OSError):
            continue
        permissions = getattr(attrs, "permissions", None)
        if permissions is None or stat_module.S_ISLNK(permissions):
            raise HpcMcpError(
                "PATH_OUTSIDE_PROJECT", "Remote path contains a symbolic link"
            )


def _reject_local_symlink(record: ProjectRecord, relative_path: str) -> None:
    requested = Path(relative_path)
    lexical = record.local_root / requested
    if lexical.is_symlink():
        raise HpcMcpError("PATH_OUTSIDE_PROJECT", "Local path is a symbolic link")


async def _rename_over(sftp: Any, source: str, target: str) -> None:
    posix_rename = getattr(sftp, "posix_rename", None)
    if posix_rename is not None:
        await _maybe_await(posix_rename(source, target))
    else:
        await _maybe_await(sftp.rename(source, target))


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


def _text_name(name: str | bytes) -> str:
    if isinstance(name, bytes):
        try:
            return name.decode("utf-8")
        except UnicodeDecodeError:
            raise HpcMcpError(
                "REMOTE_DECODE_ERROR", "Remote filename is not valid UTF-8"
            ) from None
    return name


def _normalized_relative(path: str) -> str:
    normalized = posixpath.normpath(path.replace("\\", "/"))
    return "." if normalized in {"", "."} else normalized


def _join_relative(directory: str, name: str) -> str:
    normalized = _normalized_relative(directory)
    return name if normalized == "." else posixpath.join(normalized, name)


def _remote_relative(root: str, path: str) -> str:
    if posixpath.isabs(path):
        relative = posixpath.relpath(path, root)
    else:
        relative = posixpath.normpath(path)
    if relative == ".." or relative.startswith("../"):
        raise HpcMcpError(
            "PATH_OUTSIDE_PROJECT", "Search result is outside the project root"
        )
    return relative
