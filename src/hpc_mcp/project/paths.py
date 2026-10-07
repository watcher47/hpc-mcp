import inspect
import os
import posixpath
import stat
from pathlib import Path, PurePosixPath

from asyncssh import SFTPNoSuchFile, SFTPNoSuchPath

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRecord
from hpc_mcp.ssh.protocol import SshClientProtocol


def _outside_project() -> HpcMcpError:
    return HpcMcpError(
        code="PATH_OUTSIDE_PROJECT",
        message="Path resolves outside the project root",
    )


class LocalPathGuard:
    @staticmethod
    def resolve(record: ProjectRecord, relative_path: str) -> Path:
        requested = Path(relative_path)
        if requested.is_absolute():
            raise _outside_project()

        root = record.local_root.resolve()
        candidate = (root / requested).resolve()
        try:
            common = os.path.commonpath((root, candidate))
        except ValueError:
            raise _outside_project() from None
        if Path(common) != root:
            raise _outside_project()
        return candidate


class RemotePathGuard:
    @staticmethod
    async def resolve_creation(
        client: SshClientProtocol,
        record: ProjectRecord,
        relative_path: str,
    ) -> str:
        """Resolve a creation destination using only verified existing ancestry.

        Missing components are accepted only from explicit SFTP absence checks;
        permission and transport errors propagate before any write is attempted.
        """
        parts = relative_path.split("/")
        if not relative_path or any(
            part in {"", ".", ".."} or "\x00" in part or "\\" in part
            for part in parts
        ):
            raise _outside_project()
        root = await RemotePathGuard._root(client, record)
        current = root
        async with client.sftp() as sftp:
            for index, part in enumerate(parts):
                current = posixpath.join(current, part)
                try:
                    attrs = sftp.lstat(current)
                    if inspect.isawaitable(attrs):
                        attrs = await attrs
                except (FileNotFoundError, SFTPNoSuchFile, SFTPNoSuchPath):
                    return posixpath.join(current, *parts[index + 1:])
                permissions = getattr(attrs, "permissions", None)
                if permissions is None or stat.S_ISLNK(permissions):
                    raise _outside_project()
                if index < len(parts) - 1 and not stat.S_ISDIR(permissions):
                    raise _outside_project()
                if posixpath.normpath(await client.realpath(current)) != current:
                    raise _outside_project()
        return current

    @staticmethod
    async def _root(client: SshClientProtocol, record: ProjectRecord) -> str:
        root = posixpath.normpath(record.remote_root)
        anchor = posixpath.dirname(root)
        if (
            not root.startswith("/")
            or root == "/"
            or posixpath.basename(root) in {"", ".", ".."}
        ):
            raise _outside_project()

        canonical_anchor = posixpath.normpath(await client.realpath(anchor))
        canonical_root = posixpath.normpath(await client.realpath(root))
        if (
            canonical_anchor != anchor
            or canonical_root != root
            or posixpath.dirname(canonical_root) != canonical_anchor
        ):
            raise _outside_project()
        async with client.sftp() as sftp:
            attrs = sftp.lstat(root)
            if inspect.isawaitable(attrs):
                attrs = await attrs
        permissions = getattr(attrs, "permissions", None)
        if permissions is None or not stat.S_ISDIR(permissions):
            raise _outside_project()
        return root

    @staticmethod
    async def resolve(
        client: SshClientProtocol,
        record: ProjectRecord,
        relative_path: str,
    ) -> str:
        requested = PurePosixPath(relative_path)
        if requested.is_absolute() or ".." in requested.parts:
            raise _outside_project()

        root = await RemotePathGuard._root(client, record)
        unresolved = root if str(requested) == "." else posixpath.join(root, str(requested))
        candidate = posixpath.normpath(await client.realpath(unresolved))
        try:
            common = posixpath.commonpath((root, candidate))
        except ValueError:
            raise _outside_project() from None
        if common != root:
            raise _outside_project()
        return candidate
