"""Config-scoped SSH tools; project services keep their separate permissions."""
from __future__ import annotations

import os
import posixpath
import stat
from collections.abc import Callable
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from asyncssh import SFTPNoSuchFile

from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.remote.files import _atomic_download, _atomic_upload, _redact_io_errors
from hpc_mcp.ssh.policy import GeneralCommandPolicy
from hpc_mcp.ssh.protocol import SshClientProtocol


def _denied() -> HpcMcpError:
    return HpcMcpError("PATH_NOT_ALLOWED", "Path is outside the configured allowed roots")


def _local_path(value: str, roots: list[str]) -> Path:
    # Reject drive-relative paths, device namespaces and alternate data streams,
    # including on non-Windows hosts where pathlib would treat them as filenames.
    windows = PureWindowsPath(value)
    if (not roots or value.startswith(("\\\\", "//")) or "\x00" in value
            or (windows.drive and not windows.is_absolute())
            or ":" in value[len(windows.drive):]):
        raise _denied()
    lexical = Path(value).expanduser()
    if not lexical.is_absolute() or ".." in lexical.parts:
        raise _denied()
    if os.name == "nt" and any(
        part.endswith((" ", ".")) or PureWindowsPath(part).is_reserved()
        for part in lexical.parts[1:]
    ):
        raise _denied()
    target = lexical.resolve()
    for root_value in roots:
        root = Path(root_value).expanduser()
        if not root.is_absolute():
            continue
        canonical_root = root.resolve()
        if root != canonical_root or not root.is_dir():
            continue
        try:
            if (Path(os.path.commonpath((root, lexical))) != root
                    or Path(os.path.commonpath((root, target))) != root):
                continue
        except ValueError:
            continue
        current = lexical
        while current != root:
            if current.is_symlink() or (hasattr(current, "is_junction") and current.is_junction()):
                raise _denied()
            current = current.parent
        return target
    raise _denied()


def _absolute_remote(value: str) -> str:
    if (not value.startswith("/") or value.startswith("//") or "\\" in value
            or "\x00" in value or ".." in PurePosixPath(value).parts):
        raise _denied()
    return posixpath.normpath(value)


async def _remote_path(sftp: Any, value: str, roots: list[str],
                       *, missing_leaf: bool = False) -> str:
    target = _absolute_remote(value)
    for configured in roots:
        root = _absolute_remote(configured)
        if posixpath.commonpath((root, target)) != root:
            continue
        canonical = str(await sftp.realpath(root))
        if canonical != root:
            raise _denied()
        root_attrs = await sftp.lstat(root)
        if not stat.S_ISDIR(root_attrs.permissions or 0):
            raise _denied()
        relative = PurePosixPath(posixpath.relpath(target, root))
        current = root
        for part in relative.parts:
            current = posixpath.join(current, part)
            try:
                attrs = await sftp.lstat(current)
            except (SFTPNoSuchFile, FileNotFoundError):
                if missing_leaf and current == target:
                    return target
                raise
            if stat.S_ISLNK(attrs.permissions or 0):
                raise _denied()
            if str(await sftp.realpath(current)) != current:
                raise _denied()
        return target
    raise _denied()


class GeneralSshService:
    def __init__(self, configs: ConfigRepository,
                 ssh: Callable[[str], SshClientProtocol]) -> None:
        self._configs, self._ssh = configs, ssh

    async def server_list(self) -> list[dict[str, Any]]:
        return [{"alias": config.alias, "host": config.host, "port": config.port,
                 "username": config.username, "transportMode": config.transport_mode}
                for config in self._configs.load_all().values()]

    @_redact_io_errors
    async def execute(self, cmd_string: str, server_alias: str = "default",
                      directory: str | None = None, timeout_ms: int | None = None) -> Any:
        config = self._configs.load_cluster(server_alias)
        command = GeneralCommandPolicy(config).validate(cmd_string, "/", timeout_ms)
        if not config.allowed_remote_paths:
            raise _denied()
        requested = directory if directory is not None else config.allowed_remote_paths[0]
        client = self._ssh(server_alias)
        async with client.sftp() as sftp:
            cwd = await _remote_path(sftp, requested, config.allowed_remote_paths)
            attrs = await sftp.lstat(cwd)
            if not stat.S_ISDIR(attrs.permissions or 0):
                raise HpcMcpError("REMOTE_NOT_DIRECTORY", "SSH directory must be a directory")
        return await client.run_general(cmd_string, cwd, round(command.timeout_seconds * 1000))

    @_redact_io_errors
    async def upload(self, local_path: str, remote_path: str,
                     server_alias: str = "default") -> dict[str, Any]:
        config = self._configs.load_cluster(server_alias)
        source = _local_path(local_path, config.allowed_local_paths)
        if not source.is_file():
            raise HpcMcpError("LOCAL_NOT_FILE", "Upload source must be a regular file")
        client = self._ssh(server_alias)
        async with client.sftp() as sftp:
            target = await _remote_path(sftp, remote_path, config.allowed_remote_paths, missing_leaf=True)
            try:
                attrs = await sftp.lstat(target)
            except (SFTPNoSuchFile, FileNotFoundError):
                attrs = None
            if attrs is not None and not stat.S_ISREG(attrs.permissions or 0):
                raise HpcMcpError("REMOTE_NOT_FILE", "Upload destination must be a regular file")
            size, digest = await _atomic_upload(sftp, source, target)
        return {"local_path": str(source), "remote_path": target, "size": size, "sha256": digest}

    @_redact_io_errors
    async def download(self, remote_path: str, local_path: str,
                       server_alias: str = "default") -> dict[str, Any]:
        config = self._configs.load_cluster(server_alias)
        target = _local_path(local_path, config.allowed_local_paths)
        if target.exists() and not target.is_file():
            raise HpcMcpError("LOCAL_NOT_FILE", "Download destination must be a regular file")
        client = self._ssh(server_alias)
        async with client.sftp() as sftp:
            source = await _remote_path(sftp, remote_path, config.allowed_remote_paths)
            attrs = await sftp.lstat(source)
            if not stat.S_ISREG(attrs.permissions or 0):
                raise HpcMcpError("REMOTE_NOT_FILE", "Download source must be a regular file")
            size, digest = await _atomic_download(sftp, source, target, attrs)
        return {"local_path": str(target), "remote_path": source, "size": size, "sha256": digest}
