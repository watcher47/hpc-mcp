import hashlib
import json
import posixpath
import stat
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry
from hpc_mcp.remote.files import RemoteFileService
from hpc_mcp.ssh.protocol import CommandResult


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
async def test_list_skips_sftp_dot_entries_before_metadata(tmp_path, encoded):
    sftp = FakeSftp()
    async def scandir(path):
        for name in [".", ".."]:
            yield SimpleNamespace(filename=name.encode() if encoded else name, attrs=SimpleNamespace(permissions=None))
        yield SimpleNamespace(filename="结果.txt".encode() if encoded else "结果.txt", attrs=await sftp.lstat(path + "/结果.txt"))
    sftp.scandir = scandir
    sftp.files["/srv/projects/demo/结果.txt"] = b"safe"
    files, project_id, _, _ = service(tmp_path, sftp)
    assert [entry.name for entry in await files.list(project_id)] == ["结果.txt"]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["", "/private", "../private", "a/b", "a\\b", "nul\x00name", b"\xff"])
async def test_list_rejects_unsafe_sftp_entry_before_metadata(tmp_path, name):
    sftp = FakeSftp()
    async def scandir(path):
        yield SimpleNamespace(filename=name, attrs=SimpleNamespace(permissions=None))
    original_lstat = sftp.lstat
    async def guarded_lstat(path):
        assert path == "/srv/projects/demo", "unsafe entry reached lstat"
        return await original_lstat(path)
    sftp.scandir = scandir
    sftp.lstat = guarded_lstat
    files, project_id, _, _ = service(tmp_path, sftp)
    with pytest.raises(HpcMcpError) as error:
        await files.list(project_id)
    assert error.value.code == ("REMOTE_DECODE_ERROR" if name == b"\xff" else "PATH_OUTSIDE_PROJECT")
    assert error.value.details == {}
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__
    assert "UnicodeDecodeError" not in "".join(traceback.format_exception(error.value))
    assert sftp.operations == []


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
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.modes: dict[str, int] = {}
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
        return SimpleNamespace(
            permissions=permissions,
            size=size,
            mtime=7,
            mtime_ns=42,
            uid=1000,
            gid=100,
        )

    async def scandir(self, path: str):
        prefix = path.rstrip("/") + "/"
        children: dict[str, SimpleNamespace] = {}
        for directory in self.directories:
            if directory.startswith(prefix):
                remainder = directory[len(prefix) :]
                if remainder and "/" not in remainder:
                    children[remainder] = await self.lstat(directory)
        for filename in self.files:
            if filename.startswith(prefix):
                remainder = filename[len(prefix) :]
                if remainder and "/" not in remainder:
                    children[remainder] = await self.lstat(filename)
        for name, attrs in children.items():
            yield SimpleNamespace(filename=name, attrs=attrs)

    async def makedirs(self, path: str, exist_ok: bool = False) -> None:
        current = "/"
        for part in path.strip("/").split("/"):
            current = posixpath.join(current, part)
            self.directories.add(current)
        self.operations.append(("makedirs", path))

    async def chmod(self, path: str, mode: int) -> None:
        self.modes[path] = mode
        self.operations.append(("chmod", path, mode))

    async def posix_rename(self, source: str, target: str) -> None:
        self.files[target] = self.files.pop(source)
        if source in self.modes:
            self.modes[target] = self.modes.pop(source)
        self.operations.append(("rename", source, target))

    async def remove(self, path: str) -> None:
        self.files.pop(path, None)
        self.operations.append(("remove", path))

    async def lexists(self, path: str) -> bool:
        return path in self.files or path in self.directories


class FakeSshClient:
    def __init__(self, sftp: FakeSftp, command_results: list[CommandResult] | None = None) -> None:
        self._sftp = sftp
        self.command_results = list(command_results or [])
        self.commands = []

    @asynccontextmanager
    async def sftp(self):
        yield self._sftp

    async def realpath(self, path: str) -> str:
        if posixpath.dirname(posixpath.normpath(path)) not in self._sftp.directories:
            raise FileNotFoundError(path)
        return posixpath.normpath(path)

    async def run(self, command):
        self.commands.append(command)
        return self.command_results.pop(0)


@pytest.mark.asyncio
async def test_upload_creates_missing_nested_remote_parents(tmp_path):
    sftp = FakeSftp()
    files, project_id, root, _ = service(tmp_path, sftp)
    (root / "source").write_bytes(b"safe")
    result = await files.upload(project_id, "source", "new/nested/target")
    assert result.size == 4
    assert sftp.files["/srv/projects/demo/new/nested/target"] == b"safe"
    assert all(op[1].startswith("/srv/projects/demo/") for op in sftp.operations)
def service(tmp_path: Path, sftp: FakeSftp, client: FakeSshClient | None = None):
    root = tmp_path / "demo"
    root.mkdir()
    registry = ProjectRegistry(tmp_path / "state.sqlite3")
    record = registry.ensure(root, "default", "/srv/projects")
    ssh = client or FakeSshClient(sftp)
    return RemoteFileService(registry, ssh), record.id, root, ssh


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "bytes"])
@pytest.mark.parametrize("failure", [None, "write", "close", "rename"])
async def test_remote_writes_support_sftp_handles_without_flush(tmp_path, monkeypatch, operation, failure):
    from hpc_mcp.remote.files import _write_remote_bytes

    monkeypatch.delattr(FakeRemoteFile, "flush")
    sftp = FakeSftp()
    target = "/srv/projects/demo/target"
    sftp.files[target] = b"original"
    files, project_id, root, _ = service(tmp_path, sftp)
    (root / "source").write_bytes(b"replacement")
    async def fail(*args):
        raise OSError("simulated transport failure")
    if failure in {"write", "close"}:
        monkeypatch.setattr(FakeRemoteFile, failure, fail)
    elif failure == "rename":
        sftp.posix_rename = fail
    async def perform():
        if operation == "upload":
            await files.upload(project_id, "source", "target")
        else:
            await _write_remote_bytes(sftp, target, b"replacement", 0o640)
    if failure:
        with pytest.raises(HpcMcpError if operation == "upload" else OSError):
            await perform()
        assert sftp.files == {target: b"original"}
        assert any(op[0] == "remove" for op in sftp.operations)
        assert not any(op[0] == "rename" for op in sftp.operations)
    else:
        await perform()
        assert sftp.files == {target: b"replacement"}
        rename = next(op for op in sftp.operations if op[0] == "rename")
        temporary = rename[1]
        assert sftp.operations.index(("write", temporary)) < sftp.operations.index(("close", temporary)) < sftp.operations.index(rename)


@pytest.mark.asyncio
async def test_read_caps_chunk_and_returns_full_file_hash(tmp_path: Path) -> None:
    sftp = FakeSftp()
    content = b"a" * (2 * 1024 * 1024)
    sftp.files["/srv/projects/demo/large.txt"] = content
    files, project_id, _, _ = service(tmp_path, sftp)

    result = await files.read(project_id, "large.txt")

    assert len(result.content.encode("utf-8")) == 256 * 1024
    assert result.next_offset == 256 * 1024
    assert result.sha256 == hashlib.sha256(content).hexdigest()
    assert result.truncated is True
    assert result.size == len(content)


@pytest.mark.asyncio
async def test_read_reports_invalid_utf8_explicitly(tmp_path: Path) -> None:
    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/binary.dat"] = b"valid\xffinvalid"
    files, project_id, _, _ = service(tmp_path, sftp)

    with pytest.raises(HpcMcpError) as error:
        await files.read(project_id, "binary.dat")

    assert error.value.code == "REMOTE_DECODE_ERROR"


@pytest.mark.asyncio
async def test_read_preserves_cancellation_when_handle_close_fails(tmp_path: Path) -> None:
    import asyncio

    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/cancelled.txt"] = b"content"
    sftp.read_error = asyncio.CancelledError()
    sftp.close_error = OSError("private close failure")
    files, project_id, _, _ = service(tmp_path, sftp)

    with pytest.raises(asyncio.CancelledError):
        await files.read(project_id, "cancelled.txt")

    assert ("close", "/srv/projects/demo/cancelled.txt") in sftp.operations


@pytest.mark.asyncio
async def test_list_is_sorted_and_stat_returns_posix_metadata(tmp_path: Path) -> None:
    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/z.txt"] = b"z"
    sftp.files["/srv/projects/demo/a.txt"] = b"abc"
    sftp.modes["/srv/projects/demo/a.txt"] = 0o640
    files, project_id, _, _ = service(tmp_path, sftp)

    entries = await files.list(project_id, ".")
    metadata = await files.stat(project_id, "a.txt")

    assert [entry.name for entry in entries] == ["a.txt", "z.txt"]
    assert metadata.size == 3
    assert metadata.mode == 0o640
    assert metadata.uid == 1000
    assert metadata.gid == 100
    assert metadata.kind == "file"


@pytest.mark.asyncio
async def test_paths_must_be_project_relative(tmp_path: Path) -> None:
    sftp = FakeSftp()
    files, project_id, _, _ = service(tmp_path, sftp)

    with pytest.raises(HpcMcpError) as error:
        await files.read(project_id, "../secret")

    assert error.value.code == "PATH_OUTSIDE_PROJECT"
    assert sftp.operations == []


@pytest.mark.asyncio
async def test_search_uses_structured_rg_arguments_and_caps_matches(tmp_path: Path) -> None:
    lines = []
    for number in range(1, 502):
        lines.append(
            json.dumps(
                {
                    "type": "match",
                    "data": {
                        "path": {"text": "/srv/projects/demo/main.py"},
                        "lines": {"text": "needle here\n"},
                        "line_number": number,
                        "submatches": [{"start": 0, "end": 6}],
                    },
                }
            )
        )
    sftp = FakeSftp()
    result = CommandResult(exit_status=0, stdout="\n".join(lines), stderr="")
    client = FakeSshClient(sftp, [result])
    files, project_id, _, ssh = service(tmp_path, sftp, client)

    found = await files.search(project_id, "needle", ".")

    command = ssh.commands[0]
    assert command.program == "rg"
    assert command.args[-2:] == ("needle", "/srv/projects/demo")
    assert command.args[command.args.index("--max-count") + 1] == "500"
    assert len(found.matches) == 500
    assert found.truncated is True


@pytest.mark.asyncio
async def test_search_requires_rg_instead_of_evaluating_python_source(tmp_path: Path) -> None:
    missing = CommandResult(exit_status=127, stdout="", stderr="rg: not found")
    sftp = FakeSftp()
    client = FakeSshClient(sftp, [missing])
    files, project_id, _, ssh = service(tmp_path, sftp, client)

    with pytest.raises(HpcMcpError) as error:
        await files.search(project_id, "a.*b", ".")

    assert error.value.code == "REMOTE_SEARCH_FAILED"
    assert [command.program for command in ssh.commands] == ["rg"]


@pytest.mark.asyncio
async def test_upload_and_download_replace_destinations_atomically(tmp_path: Path) -> None:
    sftp = FakeSftp()
    files, project_id, root, _ = service(tmp_path, sftp)
    source = root / "input.txt"
    source.write_text("new remote", encoding="utf-8")
    source.chmod(0o640)
    source_mode = stat.S_IMODE(source.stat().st_mode)

    uploaded = await files.upload(project_id, "input.txt", "nested/remote.txt")

    remote_target = "/srv/projects/demo/nested/remote.txt"
    rename = next(op for op in sftp.operations if op[0] == "rename")
    assert rename[2] == remote_target
    assert sftp.files[remote_target] == b"new remote"
    assert sftp.modes[remote_target] == source_mode
    assert uploaded.sha256 == hashlib.sha256(b"new remote").hexdigest()

    sftp.operations.clear()
    sftp.files[remote_target] = b"downloaded"
    downloaded = await files.download(
        project_id, "nested/remote.txt", "downloads/result.txt"
    )

    assert (root / "downloads/result.txt").read_bytes() == b"downloaded"
    assert not list((root / "downloads").glob("*.hpc-download-*"))
    assert downloaded.size == len(b"downloaded")


@pytest.mark.asyncio
@pytest.mark.asyncio
@pytest.mark.parametrize("content", [b"\xe4\xb8", b"prefix\xe4\xb8"])
async def test_incomplete_utf8_at_physical_eof_is_decode_error(tmp_path, content):
    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/file"] = content
    files, project_id, _, _ = service(tmp_path, sftp)
    with pytest.raises(HpcMcpError) as error:
        await files.read(project_id, "file")
    assert error.value.code == "REMOTE_DECODE_ERROR"


@pytest.mark.asyncio
async def test_small_multibyte_read_never_returns_nonadvancing_offset(tmp_path):
    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/file"] = "中文".encode()
    files, project_id, _, _ = service(tmp_path, sftp)
    try:
        result = await files.read(project_id, "file", limit=1)
    except HpcMcpError as error:
        assert error.code == "READ_LIMIT_TOO_SMALL"
    else:
        assert result.next_offset is None or result.next_offset > result.offset
        assert result.content


@pytest.mark.asyncio
async def test_upload_creates_private_exclusive_temp_before_final_mode(tmp_path):
    sftp = FakeSftp()
    files, project_id, root, _ = service(tmp_path, sftp)
    (root / "file").write_bytes(b"private bytes")
    (root / "file").chmod(0o640)
    source_mode = stat.S_IMODE((root / "file").stat().st_mode)
    await files.upload(project_id, "file")
    operations = sftp.operations
    opened = next(op for op in operations if op[0] == "open")
    assert opened[2:] == ("xb", 0o600)
    temp = opened[1]
    assert operations.index(opened) < operations.index(("write", temp))
    final_mode = ("chmod", temp, source_mode)
    assert operations.index(("close", temp)) < operations.index(final_mode)
    assert operations.index(final_mode) < next(i for i, op in enumerate(operations) if op[0] == "rename")


@pytest.mark.asyncio
async def test_rg_per_file_producer_cap_marks_result_truncated(tmp_path):
    events = [json.dumps({"type": "match", "data": {
        "path": {"text": "/srv/projects/demo/file"}, "lines": {"text": "needle"},
        "line_number": i + 1, "submatches": [{"start": 0}]}}) for i in range(500)]
    sftp = FakeSftp()
    client = FakeSshClient(sftp, [CommandResult(exit_status=0, stdout="\n".join(events), stderr="")])
    files, project_id, _, _ = service(tmp_path, sftp, client)
    found = await files.search(project_id, "needle")
    assert len(found.matches) == 500
    assert found.truncated


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["list", "stat", "read", "upload", "download", "search"])
@pytest.mark.parametrize("kind", ["sftp", "io", "encoding"])
async def test_file_service_errors_are_typed_and_redacted(tmp_path, monkeypatch, operation, kind):
    import asyncssh
    import traceback

    sftp = FakeSftp()
    files, project_id, root, client = service(tmp_path, sftp)
    (root / "file").write_bytes(b"content")
    secret = "/srv/private/secret replacement payload"
    exceptions = {"sftp": asyncssh.SFTPFailure(secret), "io": OSError(secret),
                  "encoding": UnicodeEncodeError("utf8", secret, 0, 1, secret)}
    async def fail(*args, **kwargs):
        raise exceptions[kind]
    monkeypatch.setattr(client, "realpath", fail)
    with pytest.raises(HpcMcpError) as error:
        await getattr(files, operation)(project_id, "file")
    assert error.value.code == ("REMOTE_ENCODING_ERROR" if kind == "encoding" else "REMOTE_IO_ERROR")
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__
    assert secret not in "".join(traceback.format_exception(error.value))
    assert secret not in str(error.value.details)


@pytest.mark.asyncio
async def test_download_works_without_fchmod_and_closes_before_chmod(tmp_path, monkeypatch):
    import os
    from hpc_mcp.remote import files as module

    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/file"] = b"download"
    files, project_id, root, _ = service(tmp_path, sftp)
    monkeypatch.delattr(module.os, "fchmod", raising=False)
    original_close, original_chmod = os.close, os.chmod
    closed = []
    def close(fd):
        closed.append(fd)
        return original_close(fd)
    def chmod(path, mode):
        assert closed, "descriptor must close before portable chmod"
        return original_chmod(path, mode)
    monkeypatch.setattr(module.os, "close", close)
    monkeypatch.setattr(module.os, "chmod", chmod)
    await files.download(project_id, "file")
    assert (root / "file").read_bytes() == b"download"


@pytest.mark.asyncio
async def test_failed_download_closes_descriptor_before_cleanup(tmp_path, monkeypatch):
    import os
    from hpc_mcp.remote import files as module

    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/file"] = b"download"
    files, project_id, root, _ = service(tmp_path, sftp)
    original_close, original_unlink = os.close, Path.unlink
    closed = []
    def close(fd):
        closed.append(fd)
        return original_close(fd)
    def unlink(path, **kwargs):
        assert closed, "Windows cannot unlink an open descriptor"
        return original_unlink(path, **kwargs)
    def fail(fd):
        raise OSError("private local path")
    monkeypatch.setattr(module.os, "close", close)
    monkeypatch.setattr(module.os, "fsync", fail)
    monkeypatch.setattr(Path, "unlink", unlink)
    with pytest.raises(HpcMcpError) as error:
        await files.download(project_id, "file")
    assert error.value.code == "REMOTE_IO_ERROR"
    assert not list(root.glob("*.hpc-download-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "download"])
async def test_transfer_temp_collision_preserves_existing_file(tmp_path, monkeypatch, operation):
    from hpc_mcp.remote import files as module

    sftp = FakeSftp()
    files, project_id, root, _ = service(tmp_path, sftp)
    (root / "file").write_bytes(b"input")
    sftp.files["/srv/projects/demo/file"] = b"remote"
    monkeypatch.setattr(module, "uuid4", lambda: SimpleNamespace(hex="collision"))
    local_temp = root / "file.hpc-download-collision"
    remote_temp = "/srv/projects/demo/file.hpc-upload-collision"
    local_temp.write_bytes(b"existing secret")
    sftp.files[remote_temp] = b"existing secret"
    with pytest.raises(HpcMcpError):
        await getattr(files, operation)(project_id, "file")
    assert local_temp.read_bytes() == b"existing secret"
    assert sftp.files.get(remote_temp) == b"existing secret"


@pytest.mark.asyncio
async def test_read_valid_multibyte_boundary_advances(tmp_path):
    sftp = FakeSftp()
    sftp.files["/srv/projects/demo/file"] = "a中文".encode()
    files, project_id, _, _ = service(tmp_path, sftp)
    first = await files.read(project_id, "file", limit=3)
    assert first.content == "a"
    assert first.next_offset == 1
    second = await files.read(project_id, "file", offset=first.next_offset, limit=3)
    assert second.content == "中"
    assert second.next_offset == 4


@pytest.mark.asyncio
async def test_upload_local_io_error_is_redacted(tmp_path, monkeypatch):
    from hpc_mcp.remote import files as module

    files, project_id, _, _ = service(tmp_path, FakeSftp())
    def fail(*args):
        raise OSError("C:/private/secret-file")
    monkeypatch.setattr(module.os, "open", fail)
    with pytest.raises(HpcMcpError) as error:
        await files.upload(project_id, "file")
    assert error.value.code == "REMOTE_IO_ERROR"
    assert error.value.__suppress_context__
    assert "private" not in str(error.value)
