import stat
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.paths import LocalPathGuard, RemotePathGuard
from hpc_mcp.project.registry import ProjectRecord


def project_record(local_root: Path, remote_root: str = "/srv/projects/demo") -> ProjectRecord:
    return ProjectRecord(
        id="project-id",
        local_root=local_root,
        server_alias="default",
        slug="demo",
        remote_root=remote_root,
        created_at="2026-09-14T00:00:00+00:00",
    )


def assert_outside_project(error: pytest.ExceptionInfo[HpcMcpError]) -> None:
    assert error.value.code == "PATH_OUTSIDE_PROJECT"


def test_local_guard_resolves_a_path_inside_project(tmp_path: Path) -> None:
    local_root = tmp_path / "project"
    local_root.mkdir()
    source = local_root / "src" / "main.py"
    source.parent.mkdir()
    source.write_text("print('safe')\n", encoding="utf-8")

    resolved = LocalPathGuard.resolve(project_record(local_root), "src/main.py")

    assert resolved == source.resolve()


@pytest.mark.parametrize("relative_path", ["../secret", "src/../../secret"])
def test_local_guard_rejects_traversal(
    tmp_path: Path, relative_path: str
) -> None:
    local_root = tmp_path / "project"
    local_root.mkdir()

    with pytest.raises(HpcMcpError) as error:
        LocalPathGuard.resolve(project_record(local_root), relative_path)

    assert_outside_project(error)


class CreationClient:
    def __init__(self):
        self.entries = {
            "/srv/projects/demo": stat.S_IFDIR | 0o755,
            "/srv/projects/demo/existing": stat.S_IFDIR | 0o755,
            "/srv/projects/demo/existing/file": stat.S_IFREG | 0o644,
        }
        self.redirects = {}
        self.error = None

    async def realpath(self, path):
        if path == "/srv/projects" or path in self.entries:
            return self.redirects.get(path, path)
        if path.rsplit("/", 1)[0] in self.entries:
            return path
        raise FileNotFoundError(path)

    async def lstat(self, path):
        if path != "/srv/projects/demo" and self.error:
            raise self.error
        if path not in self.entries:
            raise FileNotFoundError(path)
        return SimpleNamespace(permissions=self.entries[path])

    async def lexists(self, path):
        if self.error:
            raise self.error
        return path in self.entries

    @asynccontextmanager
    async def sftp(self):
        yield self


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["new/nested/file", ".hpc/manifest.json", "existing/file"])
async def test_creation_resolves_missing_and_existing_paths(tmp_path, path):
    client = CreationClient()
    assert await RemotePathGuard.resolve_creation(client, project_record(tmp_path), path) == "/srv/projects/demo/" + path


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["", "/absolute", "../escape", "a/../b", "a//b", "a/./b", "a\\b", "nul\x00name"])
async def test_creation_rejects_malformed_components_before_remote_access(tmp_path, path):
    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve_creation(FakeSshClient({}), project_record(tmp_path), path)
    assert_outside_project(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("denied"), HpcMcpError("SSH_TRANSPORT_FAILED", "transport failed")])
async def test_creation_never_falls_back_on_permission_or_transport_errors(tmp_path, failure):
    client = CreationClient()
    client.error = failure
    with pytest.raises(type(failure)) as error:
        await RemotePathGuard.resolve_creation(client, project_record(tmp_path), "new/nested/file")
    assert error.value is failure


@pytest.mark.asyncio
async def test_creation_does_not_use_lexists_which_swallows_permission_denied(tmp_path):
    from asyncssh import SFTPClient, SFTPPermissionDenied
    from asyncssh.constants import FILEXFER_TYPE_UNKNOWN
    client = CreationClient()
    async def lstat(path):
        if path == "/srv/projects/demo":
            return SimpleNamespace(permissions=stat.S_IFDIR)
        raise SFTPPermissionDenied("denied")
    client.lstat = lstat
    async def lexists(path):
        return await SFTPClient._type(client, path, statfunc=client.lstat) != FILEXFER_TYPE_UNKNOWN
    client.lexists = lexists
    with pytest.raises(SFTPPermissionDenied):
        await RemotePathGuard.resolve_creation(client, project_record(tmp_path), "new/nested/file")


@pytest.mark.asyncio
async def test_creation_never_falls_back_from_canonicalization_transport_failure(tmp_path):
    client = CreationClient()
    original = client.realpath
    failure = HpcMcpError("SSH_TRANSPORT_FAILED", "transport failed")
    async def realpath(path):
        if path == "/srv/projects/demo/existing":
            raise failure
        return await original(path)
    client.realpath = realpath
    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve_creation(client, project_record(tmp_path), "existing/new/file")
    assert error.value is failure


@pytest.mark.asyncio
@pytest.mark.parametrize("absence", ["file", "path"])
async def test_creation_accepts_explicit_sftp_no_such_responses(tmp_path, absence):
    from asyncssh import SFTPNoSuchFile, SFTPNoSuchPath
    client = CreationClient()
    original = client.lstat
    async def lstat(path):
        if path == "/srv/projects/demo/new":
            raise (SFTPNoSuchFile if absence == "file" else SFTPNoSuchPath)("missing")
        return await original(path)
    client.lstat = lstat
    assert await RemotePathGuard.resolve_creation(client, project_record(tmp_path), "new/nested/file") == "/srv/projects/demo/new/nested/file"


@pytest.mark.asyncio
@pytest.mark.parametrize("condition", ["missing-root", "symlink-root", "symlink-ancestor", "foreign-ancestor", "file-ancestor"])
async def test_creation_requires_real_root_and_confined_directory_ancestry(tmp_path, condition):
    client = CreationClient()
    root = "/srv/projects/demo"
    if condition == "missing-root":
        del client.entries[root]
    elif condition == "symlink-root":
        client.entries[root] = stat.S_IFLNK | 0o777
    elif condition == "symlink-ancestor":
        client.entries[root + "/existing"] = stat.S_IFLNK | 0o777
        client.redirects[root + "/existing"] = "/outside"
    elif condition == "foreign-ancestor":
        client.redirects[root + "/existing"] = "/outside"
    else:
        client.entries[root + "/existing"] = stat.S_IFREG | 0o644
    with pytest.raises((HpcMcpError, FileNotFoundError)):
        await RemotePathGuard.resolve_creation(client, project_record(tmp_path), "existing/new/file")


def test_local_guard_rejects_absolute_path(tmp_path: Path) -> None:
    local_root = tmp_path / "project"
    local_root.mkdir()

    with pytest.raises(HpcMcpError) as error:
        LocalPathGuard.resolve(project_record(local_root), str(tmp_path / "secret"))

    assert_outside_project(error)


def test_local_guard_rejects_symlink_pointing_outside_project(tmp_path: Path) -> None:
    local_root = tmp_path / "project"
    local_root.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")
    (local_root / "link").symlink_to(outside)

    with pytest.raises(HpcMcpError) as error:
        LocalPathGuard.resolve(project_record(local_root), "link")

    assert_outside_project(error)


class FakeSshClient:
    def __init__(
        self,
        resolved_paths: dict[str, str],
        root_permissions: int = stat.S_IFDIR | 0o755,
    ) -> None:
        self._resolved_paths = resolved_paths
        self._root_permissions = root_permissions

    async def realpath(self, path: str) -> str:
        return self._resolved_paths[path]

    @asynccontextmanager
    async def sftp(self):
        yield SimpleNamespace(lstat=self.lstat)

    async def lstat(self, path: str):
        return SimpleNamespace(permissions=self._root_permissions)


@pytest.mark.asyncio
async def test_remote_guard_returns_resolved_path_inside_project(tmp_path: Path) -> None:
    record = project_record(tmp_path, "/home/alice/codex-projects/demo")
    client = FakeSshClient(
        {
            "/home/alice/codex-projects": "/home/alice/codex-projects",
            "/home/alice/codex-projects/demo": "/home/alice/codex-projects/demo",
            "/home/alice/codex-projects/demo/output/result.txt": (
                "/home/alice/codex-projects/demo/output/result.txt"
            ),
        }
    )

    resolved = await RemotePathGuard.resolve(client, record, "output/result.txt")

    assert resolved == "/home/alice/codex-projects/demo/output/result.txt"


@pytest.mark.asyncio
async def test_remote_guard_rejects_realpath_outside_project(tmp_path: Path) -> None:
    record = project_record(tmp_path)
    client = FakeSshClient(
        {
            "/srv/projects": "/srv/projects",
            "/srv/projects/demo": "/srv/projects/demo",
            "/srv/projects/demo/link": "/etc/passwd",
        }
    )

    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve(client, record, "link")

    assert_outside_project(error)


@pytest.mark.asyncio
async def test_remote_guard_rejects_project_root_replaced_by_symlink_inside_anchor(
    tmp_path: Path,
) -> None:
    record = project_record(tmp_path, "/home/alice/codex-projects/demo")
    client = FakeSshClient(
        {
            "/home/alice/codex-projects": "/home/alice/codex-projects",
            "/home/alice/codex-projects/demo": "/home/alice/codex-projects/other",
        }
    )

    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve(client, record, ".")

    assert_outside_project(error)


@pytest.mark.asyncio
async def test_remote_guard_rejects_project_root_replaced_by_symlink_outside_anchor(
    tmp_path: Path,
) -> None:
    record = project_record(tmp_path, "/home/alice/codex-projects/demo")
    client = FakeSshClient(
        {
            "/home/alice/codex-projects": "/home/alice/codex-projects",
            "/home/alice/codex-projects/demo": "/tmp/escape",
        }
    )

    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve(client, record, ".")

    assert_outside_project(error)


@pytest.mark.asyncio
async def test_remote_guard_requires_registered_root_to_remain_a_directory(
    tmp_path: Path,
) -> None:
    root = "/home/alice/codex-projects/demo"
    client = FakeSshClient(
        {
            "/home/alice/codex-projects": "/home/alice/codex-projects",
            root: root,
        },
        root_permissions=stat.S_IFREG | 0o600,
    )

    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve(client, project_record(tmp_path, root), ".")

    assert_outside_project(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("relative_path", ["../secret", "/etc/passwd"])
async def test_remote_guard_rejects_lexically_unsafe_paths_without_remote_access(
    tmp_path: Path, relative_path: str
) -> None:
    record = project_record(tmp_path)
    client = FakeSshClient({})

    with pytest.raises(HpcMcpError) as error:
        await RemotePathGuard.resolve(client, record, relative_path)

    assert_outside_project(error)
