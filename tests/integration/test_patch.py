import hashlib
import posixpath
import stat
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry
from hpc_mcp.remote.patch import RemotePatchService, TextEdit


class FakeRemoteFile:
    def __init__(self, sftp: "FakeSftp", path: str, mode: str) -> None:
        self.sftp = sftp
        self.path = path
        self.mode = mode
        self.buffer = bytearray(sftp.files.get(path, b"")) if "r" in mode else bytearray()
        self.position = 0

    async def read(self, size: int = -1) -> bytes:
        if self.sftp.read_error is not None:
            raise self.sftp.read_error
        if size < 0:
            size = len(self.buffer) - self.position
        chunk = bytes(self.buffer[self.position : self.position + size])
        self.position += len(chunk)
        return chunk

    async def write(self, data: bytes) -> None:
        self.sftp.operations.append(("write", self.path))
        self.buffer.extend(data)

    async def flush(self) -> None:
        self.sftp.operations.append(("flush", self.path))

    async def close(self) -> None:
        if "w" in self.mode or "x" in self.mode:
            self.sftp.files[self.path] = bytes(self.buffer)
        self.sftp.operations.append(("close", self.path))
        if self.sftp.close_error is not None:
            raise self.sftp.close_error


class FakeSftp:
    def __init__(self, content: bytes, mode: int = 0o640) -> None:
        self.target = "/srv/projects/demo/main.py"
        self.files = {self.target: content}
        self.modes = {self.target: mode}
        self.directories = {"/srv", "/srv/projects", "/srv/projects/demo"}
        self.operations: list[tuple] = []
        self.read_error: BaseException | None = None
        self.close_error: BaseException | None = None

    async def open(self, path: str, mode: str, attrs=None) -> FakeRemoteFile:
        self.operations.append(("open", path, mode, getattr(attrs, "permissions", None)))
        if "x" in mode:
            if path in self.files:
                raise FileExistsError(path)
            self.files[path] = b""
            self.modes[path] = attrs.permissions
        if "r" in mode and path not in self.files:
            raise FileNotFoundError(path)
        return FakeRemoteFile(self, path, mode)

    async def lstat(self, path: str):
        if path in self.directories:
            permissions = stat.S_IFDIR | 0o755
            size = 0
        elif path in self.files:
            permissions = stat.S_IFREG | self.modes.get(path, 0o644)
            size = len(self.files[path])
        else:
            raise FileNotFoundError(path)
        return SimpleNamespace(permissions=permissions, size=size, mtime=1, mtime_ns=0)

    async def makedirs(self, path: str, exist_ok: bool = False) -> None:
        self.operations.append(("makedirs", path))

    async def chmod(self, path: str, mode: int) -> None:
        self.modes[path] = mode
        self.operations.append(("chmod", path, mode))

    async def posix_rename(self, source: str, target: str) -> None:
        self.files[target] = self.files.pop(source)
        self.modes[target] = self.modes.pop(source)
        self.operations.append(("rename", source, target))

    async def remove(self, path: str) -> None:
        self.files.pop(path, None)
        self.operations.append(("remove", path))

    async def lexists(self, path: str) -> bool:
        return path in self.files or path in self.directories


class FakeSshClient:
    def __init__(self, sftp: FakeSftp) -> None:
        self._sftp = sftp

    @asynccontextmanager
    async def sftp(self):
        yield self._sftp

    async def realpath(self, path: str) -> str:
        return posixpath.normpath(path)


@pytest.fixture
def patch_fixture(tmp_path: Path):
    root = tmp_path / "demo"
    root.mkdir()
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    record = registry.ensure(root, "default", "/srv/projects")
    sftp = FakeSftp(b"x = 1\nx = 1\n")
    return RemotePatchService(registry, FakeSshClient(sftp)), record.id, sftp


@pytest.mark.asyncio
async def test_patch_rejects_stale_sha(patch_fixture) -> None:
    patch_service, project_id, sftp = patch_fixture

    with pytest.raises(HpcMcpError) as error:
        await patch_service.apply(
            project_id=project_id,
            relative_path="main.py",
            expected_sha256="stale",
            edits=[TextEdit(old_text="x = 1", new_text="x = 2")],
        )

    assert error.value.code == "REMOTE_CHANGED"
    assert not any(operation[0] == "write" for operation in sftp.operations)


@pytest.mark.asyncio
async def test_patch_rejects_an_inexact_edit_count_before_writing(patch_fixture) -> None:
    patch_service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()

    with pytest.raises(HpcMcpError) as error:
        await patch_service.apply(
            project_id,
            "main.py",
            expected,
            [TextEdit(old_text="x = 1", new_text="x = 2", expected_count=1)],
        )

    assert error.value.code == "EDIT_COUNT_MISMATCH"
    assert error.value.details == {"edit_index": 0, "expected": 1, "actual": 2}
    assert not any(operation[0] == "write" for operation in sftp.operations)


@pytest.mark.asyncio
async def test_patch_applies_in_order_copies_mode_and_renames_atomically(
    patch_fixture,
) -> None:
    patch_service, project_id, sftp = patch_fixture
    old = sftp.files[sftp.target]
    expected = hashlib.sha256(old).hexdigest()

    result = await patch_service.apply(
        project_id,
        "main.py",
        expected,
        [
            TextEdit(old_text="x = 1", new_text="value = 2", expected_count=2),
            TextEdit(old_text="value = 2", new_text="done = 3", expected_count=2),
        ],
    )

    updated = b"done = 3\ndone = 3\n"
    assert sftp.files[sftp.target] == updated
    rename_index = next(i for i, op in enumerate(sftp.operations) if op[0] == "rename")
    rename = sftp.operations[rename_index]
    temporary = rename[1]
    assert sftp.operations.index(("flush", temporary)) < rename_index
    assert sftp.operations.index(("close", temporary)) < rename_index
    assert sftp.operations.index(("chmod", temporary, 0o640)) < rename_index
    assert result.old_sha256 == expected
    assert result.new_sha256 == hashlib.sha256(updated).hexdigest()
    assert result.changed_bytes > 0


@pytest.mark.asyncio
async def test_patch_requires_project_relative_path(patch_fixture) -> None:
    patch_service, project_id, _ = patch_fixture

    with pytest.raises(HpcMcpError) as error:
        await patch_service.apply(project_id, "../main.py", "unused", [])

    assert error.value.code == "PATH_OUTSIDE_PROJECT"


def test_text_edit_rejects_empty_old_text() -> None:
    with pytest.raises(ValueError):
        TextEdit(old_text="", new_text="anything")


@pytest.mark.asyncio
async def test_patch_temp_is_private_exclusive_before_writes(patch_fixture):
    service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    await service.apply(project_id, "main.py", expected, [TextEdit("x = 1", "x = 2", 2)])
    opened = next(op for op in sftp.operations if op[0] == "open" and ".hpc-patch-" in op[1])
    assert opened[2:] == ("xb", 0o600)
    temp = opened[1]
    assert sftp.operations.index(opened) < sftp.operations.index(("write", temp))
    assert sftp.operations.index(("close", temp)) < sftp.operations.index(("chmod", temp, 0o640))


@pytest.mark.asyncio
async def test_patch_encoding_error_redacts_replacement(patch_fixture):
    import traceback

    service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    replacement = "SECRET\ud800"
    with pytest.raises(HpcMcpError) as error:
        await service.apply(project_id, "main.py", expected, [TextEdit("x = 1", replacement, 2)])
    assert error.value.code == "REMOTE_ENCODING_ERROR"
    assert error.value.__suppress_context__
    assert "SECRET" not in "".join(traceback.format_exception(error.value))


@pytest.mark.asyncio
async def test_patch_remote_error_is_redacted(patch_fixture, monkeypatch):
    import asyncssh
    import traceback

    service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    async def fail(*args):
        raise asyncssh.SFTPFailure("/private/remote sensitive payload")
    monkeypatch.setattr(sftp, "chmod", fail)
    with pytest.raises(HpcMcpError) as error:
        await service.apply(project_id, "main.py", expected, [TextEdit("x = 1", "new", 2)])
    assert error.value.code == "REMOTE_IO_ERROR"
    assert error.value.__suppress_context__
    assert "/private/remote" not in "".join(traceback.format_exception(error.value))
    assert not any(".hpc-patch-" in name for name in sftp.files)


@pytest.mark.asyncio
@pytest.mark.parametrize("empty_edits", [True, False])
async def test_repetitive_patch_has_bounded_work(patch_fixture, empty_edits):
    import sys

    service, project_id, sftp = patch_fixture
    sftp.files[sftp.target] = b"a" * 10000
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    edits = [] if empty_edits else [TextEdit("a" * 10000, "a" * 9999 + "b")]
    steps = 0
    def trace(frame, event, arg):
        nonlocal steps
        if event == "line":
            steps += 1
            assert steps < 250000, "patch exceeded linear work budget on repetitive bytes"
        return trace
    previous = sys.gettrace()
    try:
        sys.settrace(trace)
        result = await service.apply(project_id, "main.py", expected, edits)
    finally:
        sys.settrace(previous)
    assert result.changed_bytes == (0 if empty_edits else 1)
    if empty_edits:
        assert not any(op[0] == "write" for op in sftp.operations)


@pytest.mark.asyncio
async def test_patch_temp_collision_preserves_existing_file(patch_fixture, monkeypatch):
    from hpc_mcp.remote import files as module

    service, project_id, sftp = patch_fixture
    monkeypatch.setattr(module, "uuid4", lambda: SimpleNamespace(hex="collision"))
    temporary = sftp.target + ".hpc-patch-collision"
    sftp.files[temporary] = b"existing secret"
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    with pytest.raises(HpcMcpError):
        await service.apply(project_id, "main.py", expected, [TextEdit("x = 1", "new", 2)])
    assert sftp.files.get(temporary) == b"existing secret"


@pytest.mark.asyncio
async def test_patch_preserves_cancellation_and_cleans_temp(patch_fixture, monkeypatch):
    import asyncio

    service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    async def cancel(*args):
        raise asyncio.CancelledError()
    monkeypatch.setattr(sftp, "chmod", cancel)
    with pytest.raises(asyncio.CancelledError):
        await service.apply(project_id, "main.py", expected, [TextEdit("x = 1", "new", 2)])
    assert not any(".hpc-patch-" in path for path in sftp.files)


@pytest.mark.asyncio
async def test_patch_preserves_cancellation_when_initial_read_close_fails(patch_fixture):
    import asyncio

    service, project_id, sftp = patch_fixture
    expected = hashlib.sha256(sftp.files[sftp.target]).hexdigest()
    sftp.read_error = asyncio.CancelledError()
    sftp.close_error = OSError("private close failure")

    with pytest.raises(asyncio.CancelledError):
        await service.apply(project_id, "main.py", expected, [])

    assert ("close", sftp.target) in sftp.operations
