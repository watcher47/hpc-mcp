import hashlib
import asyncio
import json
import os
import posixpath
import stat
import traceback
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.manifest import ProjectManifest, SyncPlan
from hpc_mcp.project.registry import ProjectRecord
from hpc_mcp.project.sync import ProjectSyncService, _scan_remote_manifest


class DiskSftp:
    """Exercise exclusive creation and cleanup against real filesystem semantics."""

    def __init__(self, failure=None):
        self.failure = failure
        self.removed = []
        self.opened = []

    def makedirs(self, path, exist_ok=False):
        Path(path).mkdir(parents=True, exist_ok=exist_ok)

    def open(self, path, mode, *, attrs=None):
        self.opened.append((mode, getattr(attrs, "permissions", None)))
        flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_BINARY", 0)
        flags |= os.O_EXCL if "x" in mode else os.O_TRUNC
        descriptor = os.open(path, flags, getattr(attrs, "permissions", None) or 0o666)
        stream = os.fdopen(descriptor, "wb")
        if self.failure is None:
            return stream  # Deliberately synchronous adapter; existing tests cover no flush.
        failure = self.failure
        class FailingHandle:
            def write(self, data):
                stream.write(data[:1])
                stream.flush()
                raise failure
            def close(self):
                stream.close()
        return FailingHandle()

    def rename(self, source, target):
        os.replace(source, target)

    def remove(self, path):
        self.removed.append(path)
        Path(path).unlink()


@pytest.fixture
def atomic_sync_case(tmp_path, monkeypatch):
    from hpc_mcp.project import sync
    monkeypatch.setattr(sync, "uuid4", lambda: SimpleNamespace(hex="collision"))
    source = tmp_path / "source"
    source.write_bytes(b"replacement")
    expected = ProjectManifest.scan(tmp_path).files["source"]
    target = tmp_path / "remote" / "target"
    target.parent.mkdir()
    target.write_bytes(b"original")
    temporary = Path(str(target) + ".hpc-upload-collision")
    async def perform(sftp, operation, entry=expected):
        if operation == "upload":
            await sync._atomic_upload(sftp, source, str(target), entry)
        else:
            await sync._atomic_write_bytes(sftp, b"replacement", str(target))
    return perform, target, temporary, expected


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "manifest"])
@pytest.mark.parametrize("symlink", [False, True])
async def test_atomic_sync_collision_preserves_unowned_temp(atomic_sync_case, operation, symlink):
    perform, target, temporary, _ = atomic_sync_case
    sentinel = temporary.with_name("sentinel")
    sentinel.write_bytes(b"sentinel bytes")
    if symlink:
        try:
            temporary.symlink_to(sentinel)
        except OSError:
            pytest.skip("platform cannot create symlinks")
    else:
        temporary.write_bytes(b"sentinel bytes")
    sftp = DiskSftp()
    with pytest.raises(FileExistsError):
        await perform(sftp, operation)
    assert temporary.read_bytes() == b"sentinel bytes"
    assert sentinel.read_bytes() == b"sentinel bytes"
    assert temporary.is_symlink() == symlink
    assert target.read_bytes() == b"original"
    assert sftp.removed == []


@pytest.mark.asyncio
async def test_atomic_sync_validation_does_not_remove_unowned_temp(atomic_sync_case):
    perform, target, temporary, expected = atomic_sync_case
    temporary.write_bytes(b"sentinel bytes")
    sftp = DiskSftp()
    with pytest.raises(ValueError, match="changed after planning"):
        await perform(sftp, "upload", replace(expected, size=expected.size + 1))
    assert temporary.read_bytes() == b"sentinel bytes"
    assert target.read_bytes() == b"original"
    assert sftp.removed == []
    assert sftp.opened == []


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "manifest"])
@pytest.mark.parametrize("failure", [None, OSError("simulated write failure"), asyncio.CancelledError()])
async def test_atomic_sync_owned_temp_publication_and_cleanup(atomic_sync_case, operation, failure):
    perform, target, temporary, _ = atomic_sync_case
    sftp = DiskSftp(failure)
    if failure is None:
        await perform(sftp, operation)
        assert target.read_bytes() == b"replacement"
        assert sftp.removed == []
    else:
        with pytest.raises(type(failure)):
            await perform(sftp, operation)
        assert target.read_bytes() == b"original"
        assert sftp.removed == [str(temporary)]
    assert not temporary.exists()
    assert sftp.opened == [("xb", 0o600)]


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", [False, True])
@pytest.mark.parametrize("nested", [False, True])
async def test_scan_skips_sftp_dot_entries_before_consuming_metadata(tmp_path, encoded, nested):
    sftp = FakeSftp()
    root = "/srv/projects/demo"
    visited = []
    async def scandir(path):
        visited.append(path)
        assert path in {root, root + "/nested"}
        for name in [".", ".."]:
            yield SimpleNamespace(filename=name.encode() if encoded else name, attrs=SimpleNamespace(permissions=stat.S_IFDIR | 0o755))
        if nested and path == root:
            yield SimpleNamespace(filename=b"nested" if encoded else "nested", attrs=SimpleNamespace(permissions=stat.S_IFDIR))
        elif nested:
            yield SimpleNamespace(filename="结果.txt".encode() if encoded else "结果.txt", attrs=SimpleNamespace(permissions=stat.S_IFREG, mtime_ns=1))
    sftp.scandir = scandir
    sftp.files[root + "/nested/结果.txt"] = b"safe"
    manifest = await _scan_remote_manifest(sftp, root, tmp_path)
    assert list(manifest.files) == (["nested/结果.txt"] if nested else [])
    if nested:
        assert manifest.files["nested/结果.txt"].sha256 == hashlib.sha256(b"safe").hexdigest()
    assert visited == ([root, root + "/nested"] if nested else [root])


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["", "/private", "../private", "a/b", "a\\b", "nul\x00name", b"\xff"])
async def test_scan_rejects_unsafe_sftp_entry_before_metadata(tmp_path, name):
    sftp = FakeSftp()
    async def scandir(path):
        yield SimpleNamespace(filename=name, attrs=SimpleNamespace(permissions=None))
    async def forbidden_lstat(path):
        pytest.fail("unsafe entry reached lstat")
    sftp.scandir = scandir
    sftp.lstat = forbidden_lstat
    with pytest.raises(HpcMcpError) as error:
        await _scan_remote_manifest(sftp, "/srv/projects/demo", tmp_path)
    assert error.value.code == ("REMOTE_DECODE_ERROR" if name == b"\xff" else "PATH_OUTSIDE_PROJECT")
    assert error.value.details == {}
    assert error.value.__cause__ is None
    assert error.value.__suppress_context__
    assert "UnicodeDecodeError" not in "".join(traceback.format_exception(error.value))
    assert sftp.operations == []


def project_record(local_root: Path) -> ProjectRecord:
    return ProjectRecord(
        id="project-id",
        local_root=local_root,
        server_alias="default",
        slug="demo",
        remote_root="/srv/projects/demo",
        created_at="2026-09-14T00:00:00+00:00",
    )


class FakeRemoteFile:
    def __init__(self, sftp: "FakeSftp", path: str, mode: str) -> None:
        self.sftp = sftp
        self.path = path
        self.mode = mode
        self.buffer = bytearray(sftp.files.get(path, b"")) if "r" in mode else bytearray()
        self.position = 0

    async def read(self, size: int = -1) -> bytes:
        if size < 0:
            size = len(self.buffer) - self.position
        data = bytes(self.buffer[self.position : self.position + size])
        self.position += len(data)
        return data

    async def write(self, data: bytes) -> None:
        self.sftp.operations.append(("write", self.path))
        self.buffer.extend(data)

    async def flush(self) -> None:
        self.sftp.operations.append(("flush", self.path))

    async def close(self) -> None:
        if "w" in self.mode or "x" in self.mode:
            self.sftp.files[self.path] = bytes(self.buffer)
            self.sftp.operations.append(("close", self.path))


class FakeSftpNoSuchFile(FileNotFoundError):
    pass


class FakeSftp:
    def __init__(self, *, fail_target: str | None = None) -> None:
        self.files: dict[str, bytes] = {}
        self.directories = {"/srv", "/srv/projects", "/srv/projects/demo"}
        self.symlinks: dict[str, str] = {}
        self.operations: list[tuple[str, str, str] | tuple[str, str]] = []
        self.fail_target = fail_target
        self.after_next_rename = None

    async def open(self, path: str, mode: str, *, attrs=None) -> FakeRemoteFile:
        self.operations.append(("open", path))
        if "x" in mode and (path in self.files or path in self.symlinks):
            raise FileExistsError(path)
        if "r" in mode and path not in self.files:
            raise FileNotFoundError(path)
        return FakeRemoteFile(self, path, mode)

    async def makedirs(self, path: str, exist_ok: bool = False) -> None:
        current = "/"
        for part in path.strip("/").split("/"):
            current = posixpath.join(current, part)
            self.directories.add(current)
        self.operations.append(("makedirs", path))

    async def rename(self, source: str, target: str) -> None:
        self.operations.append(("rename", source, target))
        if target == self.fail_target:
            raise OSError("simulated rename failure")
        self.files[target] = self.files.pop(source)
        if self.after_next_rename is not None:
            callback = self.after_next_rename
            self.after_next_rename = None
            callback(source, target)

    async def remove(self, path: str) -> None:
        self.files.pop(path, None)
        self.operations.append(("remove", path))

    async def exists(self, path: str) -> bool:
        return path in self.files or path in self.directories or path in self.symlinks

    async def lstat(self, path: str):
        if path in self.symlinks:
            permissions = stat.S_IFLNK | 0o777
        elif path in self.directories:
            permissions = stat.S_IFDIR | 0o755
        elif path in self.files:
            permissions = stat.S_IFREG | 0o644
        else:
            raise FakeSftpNoSuchFile(path)
        return SimpleNamespace(permissions=permissions, mtime=1, mtime_ns=1_000_000_000)

    async def lexists(self, path: str) -> bool:
        return await self.exists(path)

    async def scandir(self, path: str):
        prefix = path.rstrip("/") + "/"
        children: dict[str, int] = {}
        for directory in self.directories:
            if directory.startswith(prefix):
                remainder = directory[len(prefix) :]
                if remainder and "/" not in remainder:
                    children[remainder] = stat.S_IFDIR | 0o755
        for filename in self.files:
            if filename.startswith(prefix):
                remainder = filename[len(prefix) :]
                if remainder and "/" not in remainder:
                    children[remainder] = stat.S_IFREG | 0o644
        for symlink in self.symlinks:
            if symlink.startswith(prefix):
                remainder = symlink[len(prefix) :]
                if remainder and "/" not in remainder:
                    children[remainder] = stat.S_IFLNK | 0o777
        for name, permissions in sorted(children.items()):
            size = len(self.files.get(posixpath.join(path, name), b""))
            yield SimpleNamespace(
                filename=name,
                attrs=SimpleNamespace(
                    permissions=permissions,
                    size=size,
                    mtime=1,
                    mtime_ns=1_000_000_000,
                ),
            )


class FakeSshClient:
    def __init__(self, sftp: FakeSftp) -> None:
        self._sftp = sftp
        self.before_target_realpath = None

    @asynccontextmanager
    async def sftp(self):
        yield self._sftp

    async def realpath(self, path: str) -> str:
        normalized = posixpath.normpath(path)
        if normalized != "/srv/projects/demo" and self.before_target_realpath:
            callback = self.before_target_realpath
            self.before_target_realpath = None
            callback()
        for source, target in sorted(
            self._sftp.symlinks.items(), key=lambda item: len(item[0]), reverse=True
        ):
            if normalized == source or normalized.startswith(f"{source}/"):
                suffix = normalized[len(source) :].lstrip("/")
                return posixpath.normpath(posixpath.join(target, suffix))
        if posixpath.dirname(normalized) not in self._sftp.directories:
            raise FileNotFoundError(normalized)
        return normalized


@pytest.mark.asyncio
async def test_sync_creates_multiple_missing_parents_before_publishing_manifest(tmp_path):
    source = tmp_path / "logs" / "nested" / ".keep"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"keep")
    sftp = FakeSftp()
    result = await ProjectSyncService(FakeSshClient(sftp)).execute(project_record(tmp_path))
    assert result.failed == []
    assert result.uploaded == ["logs/nested/.keep"]
    assert result.manifest_updated
    assert sftp.files["/srv/projects/demo/logs/nested/.keep"] == b"keep"
    renames = [op for op in sftp.operations if op[0] == "rename"]
    assert renames[-1][2] == "/srv/projects/demo/.hpc/manifest.json"
    assert all(op[1].startswith("/srv/projects/demo/") for op in sftp.operations)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["upload", "manifest"])
@pytest.mark.parametrize("failure", [None, "write", "close", "rename"])
async def test_sync_writes_support_sftp_handles_without_flush(tmp_path, monkeypatch, operation, failure):
    from hpc_mcp.project.sync import _atomic_upload, _atomic_write_bytes

    monkeypatch.delattr(FakeRemoteFile, "flush")
    sftp = FakeSftp()
    target = "/srv/projects/demo/target"
    sftp.files[target] = b"original"
    source = tmp_path / "source"
    source.write_bytes(b"replacement")
    expected = ProjectManifest.scan(tmp_path).files["source"]
    async def fail(*args):
        raise OSError("simulated transport failure")
    if failure in {"write", "close"}:
        monkeypatch.setattr(FakeRemoteFile, failure, fail)
    elif failure == "rename":
        sftp.rename = fail
    async def perform():
        if operation == "upload":
            await _atomic_upload(sftp, source, target, expected)
        else:
            await _atomic_write_bytes(sftp, b"replacement", target)
    if failure:
        with pytest.raises(OSError):
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
async def test_remote_scan_does_not_allow_hpcignore_to_negate_fixed_exclusions(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / ".hpcignore").write_text(
        "!.git/config\n!pkg/__pycache__/cache.pyc\n!pkg/module.pyc\n",
        encoding="utf-8",
    )
    sftp = FakeSftp()
    sftp.directories.update(
        {
            "/srv/projects/demo/.git",
            "/srv/projects/demo/pkg",
            "/srv/projects/demo/pkg/__pycache__",
        }
    )
    sftp.files.update(
        {
            "/srv/projects/demo/.git/config": b"secret",
            "/srv/projects/demo/pkg/__pycache__/cache.pyc": b"cache",
            "/srv/projects/demo/pkg/module.pyc": b"bytecode",
            "/srv/projects/demo/pkg/module.py": b"safe\n",
        }
    )

    manifest = await _scan_remote_manifest(sftp, "/srv/projects/demo", root)

    assert sorted(manifest.files) == ["pkg/module.py"]


@pytest.mark.asyncio
async def test_plan_uses_previous_local_and_current_remote_manifests(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "main.py").write_text("local", encoding="utf-8")
    (root / "new.txt").write_text("new", encoding="utf-8")
    record = project_record(root)
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files["/srv/projects/demo/main.py"] = b"remote"
    previous = ProjectManifest(
        files={"main.py": ProjectManifest.scan(root).files["new.txt"]}
    )
    sftp.files["/srv/projects/demo/.hpc/manifest.json"] = previous.to_json().encode()
    service = ProjectSyncService(FakeSshClient(sftp))

    plan = await service.plan(record)

    assert plan.upload == ["new.txt"]
    assert plan.conflicts == ["main.py"]


@pytest.mark.asyncio
async def test_execute_renames_uploaded_file_before_manifest_update(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "main.py").write_text("print('new')\n", encoding="utf-8")
    record = project_record(root)
    sftp = FakeSftp()
    service = ProjectSyncService(FakeSshClient(sftp))

    result = await service.execute(
        record,
        SyncPlan(
            upload=["main.py"],
            conflicts=[],
            local_manifest=ProjectManifest.scan(root),
        ),
    )

    file_rename_index = next(
        index
        for index, operation in enumerate(sftp.operations)
        if operation[0] == "rename" and operation[2].endswith("/main.py")
    )
    manifest_rename_index = next(
        index
        for index, operation in enumerate(sftp.operations)
        if operation[0] == "rename" and operation[2].endswith("/.hpc/manifest.json")
    )
    file_temp = sftp.operations[file_rename_index][1]
    assert sftp.operations.index(("flush", file_temp)) < file_rename_index
    assert sftp.operations.index(("close", file_temp)) < file_rename_index
    assert file_rename_index < manifest_rename_index
    assert result.uploaded == ["main.py"]
    assert result.failed == []
    assert result.manifest_updated is True
    persisted = json.loads(sftp.files["/srv/projects/demo/.hpc/manifest.json"])
    assert persisted == json.loads(ProjectManifest.scan(root).to_json())


@pytest.mark.asyncio
async def test_execute_rechecks_remote_state_and_does_not_apply_a_stale_plan(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "main.py"
    source.write_text("old\n", encoding="utf-8")
    previous = ProjectManifest.scan(root)
    source.write_text("local change\n", encoding="utf-8")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    remote_path = "/srv/projects/demo/main.py"
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files[manifest_path] = previous.to_json().encode()
    sftp.files[remote_path] = b"old\n"
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = await service.plan(record)
    previous_manifest = sftp.files[manifest_path]

    sftp.files[remote_path] = b"remote change\n"
    result = await service.execute(record, plan)

    assert result.uploaded == []
    assert result.conflicts == ["main.py"]
    assert result.manifest_updated is False
    assert sftp.files[remote_path] == b"remote change\n"
    assert sftp.files[manifest_path] == previous_manifest


@pytest.mark.asyncio
async def test_execute_revalidates_each_remote_file_immediately_before_upload(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    first_source = root / "a.txt"
    second_source = root / "b.txt"
    first_source.write_bytes(b"old a\n")
    second_source.write_bytes(b"old b\n")
    previous = ProjectManifest.scan(root)
    first_source.write_bytes(b"local a\n")
    second_source.write_bytes(b"local b\n")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    first_remote_path = "/srv/projects/demo/a.txt"
    second_remote_path = "/srv/projects/demo/b.txt"
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files[manifest_path] = previous.to_json().encode()
    sftp.files[first_remote_path] = b"old a\n"
    sftp.files[second_remote_path] = b"old b\n"
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = await service.plan(record)
    previous_manifest = sftp.files[manifest_path]

    def change_second_remote_after_first_upload(source: str, target: str) -> None:
        assert target == first_remote_path
        sftp.files[second_remote_path] = b"concurrent remote change\n"

    sftp.after_next_rename = change_second_remote_after_first_upload
    result = await service.execute(record, plan)

    assert result.uploaded == ["a.txt"]
    assert result.failed == []
    assert result.conflicts == ["b.txt"]
    assert result.manifest_updated is False
    assert sftp.files[first_remote_path] == b"local a\n"
    assert sftp.files[second_remote_path] == b"concurrent remote change\n"
    assert sftp.files[manifest_path] == previous_manifest
    assert not any(
        operation[0] == "write"
        and operation[1].startswith(f"{second_remote_path}.hpc-upload-")
        for operation in sftp.operations
    )


@pytest.mark.asyncio
async def test_execute_never_uploads_a_path_also_listed_as_a_conflict(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "main.py"
    source.write_text("old\n", encoding="utf-8")
    previous = ProjectManifest.scan(root)
    source.write_text("local change\n", encoding="utf-8")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    remote_path = "/srv/projects/demo/main.py"
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files[manifest_path] = previous.to_json().encode()
    sftp.files[remote_path] = b"old\n"
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = replace(await service.plan(record), conflicts=["main.py"])

    result = await service.execute(record, plan)

    assert result.uploaded == []
    assert result.conflicts == ["main.py"]
    assert result.manifest_updated is False
    assert sftp.files[remote_path] == b"old\n"


@pytest.mark.asyncio
async def test_execute_aborts_if_local_content_changed_after_planning(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "main.py"
    source.write_text("old\n", encoding="utf-8")
    previous = ProjectManifest.scan(root)
    source.write_text("planned\n", encoding="utf-8")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    remote_path = "/srv/projects/demo/main.py"
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files[manifest_path] = previous.to_json().encode()
    sftp.files[remote_path] = b"old\n"
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = await service.plan(record)
    previous_manifest = sftp.files[manifest_path]

    source.write_text("changed after plan\n", encoding="utf-8")
    result = await service.execute(record, plan)

    assert result.uploaded == []
    assert result.failed == ["main.py"]
    assert result.manifest_updated is False
    assert sftp.files[remote_path] == b"old\n"
    assert sftp.files[manifest_path] == previous_manifest
    assert not any(operation[0] == "write" for operation in sftp.operations)


@pytest.mark.asyncio
async def test_execute_verifies_source_hash_against_planned_snapshot(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    source = root / "main.py"
    source.write_bytes(b"old-old\n")
    previous = ProjectManifest.scan(root)
    source.write_bytes(b"planned\n")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    remote_path = "/srv/projects/demo/main.py"
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/.hpc")
    sftp.files[manifest_path] = previous.to_json().encode()
    sftp.files[remote_path] = b"old-old\n"
    client = FakeSshClient(sftp)
    service = ProjectSyncService(client)
    plan = await service.plan(record)
    planned_entry = plan.local_manifest.files["main.py"]
    previous_manifest = sftp.files[manifest_path]

    def replace_source_after_scan() -> None:
        source.write_bytes(b"changed\n")
        os.utime(
            source,
            ns=(planned_entry.mtime_ns, planned_entry.mtime_ns),
        )

    client.before_target_realpath = replace_source_after_scan
    result = await service.execute(record, plan)

    assert result.uploaded == []
    assert result.failed == ["main.py"]
    assert result.manifest_updated is False
    assert sftp.files[remote_path] == b"old-old\n"
    assert sftp.files[manifest_path] == previous_manifest
    assert not any(
        operation[0] == "rename" and operation[2] == remote_path
        for operation in sftp.operations
    )


@pytest.mark.asyncio
async def test_execute_rejects_in_root_remote_directory_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    (root / "linked").mkdir(parents=True)
    (root / "linked" / "main.py").write_text("local\n", encoding="utf-8")
    record = project_record(root)
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/actual")
    sftp.symlinks["/srv/projects/demo/linked"] = "/srv/projects/demo/actual"
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = await service.plan(record)

    result = await service.execute(record, plan)

    assert result.uploaded == []
    assert result.failed == ["linked/main.py"]
    assert result.manifest_updated is False
    assert "/srv/projects/demo/actual/main.py" not in sftp.files
    assert not any(operation[0] == "write" for operation in sftp.operations)


@pytest.mark.asyncio
async def test_execute_rejects_manifest_write_through_in_root_directory_symlink(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    record = project_record(root)
    sftp = FakeSftp()
    sftp.directories.add("/srv/projects/demo/actual")
    sftp.symlinks["/srv/projects/demo/.hpc"] = "/srv/projects/demo/actual"
    service = ProjectSyncService(FakeSshClient(sftp))

    result = await service.execute(
        record,
        SyncPlan(upload=[], conflicts=[], local_manifest=ProjectManifest()),
    )

    assert result.uploaded == []
    assert result.failed == [".hpc/manifest.json"]
    assert result.manifest_updated is False
    assert not any(
        operation[0] in {"open", "write", "rename"}
        and "manifest.json" in operation[-1]
        for operation in sftp.operations
    )


@pytest.mark.asyncio
async def test_execute_allows_a_missing_remote_directory(tmp_path: Path) -> None:
    root = tmp_path / "project"
    (root / "new-directory").mkdir(parents=True)
    (root / "new-directory" / "main.py").write_bytes(b"local\n")
    record = project_record(root)
    sftp = FakeSftp()
    service = ProjectSyncService(FakeSshClient(sftp))
    plan = await service.plan(record)

    result = await service.execute(record, plan)

    assert result.uploaded == ["new-directory/main.py"]
    assert result.failed == []
    assert result.manifest_updated is True
    assert sftp.files["/srv/projects/demo/new-directory/main.py"] == b"local\n"


@pytest.mark.asyncio
async def test_execute_retains_previous_manifest_after_partial_failure(
    tmp_path: Path,
) -> None:
    root = tmp_path / "project"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    (root / "broken.txt").write_text("broken", encoding="utf-8")
    record = project_record(root)
    manifest_path = "/srv/projects/demo/.hpc/manifest.json"
    sftp = FakeSftp(fail_target="/srv/projects/demo/broken.txt")
    sftp.files[manifest_path] = b'{"files":{"old.txt":{"size":3,"mtime_ns":1,"sha256":"old"}}}'
    previous_manifest = sftp.files[manifest_path]
    service = ProjectSyncService(FakeSshClient(sftp))

    result = await service.execute(
        record,
        SyncPlan(
            upload=["a.txt", "broken.txt"],
            conflicts=[],
            local_manifest=ProjectManifest.scan(root),
        ),
    )

    assert result.uploaded == ["a.txt"]
    assert result.failed == ["broken.txt"]
    assert result.manifest_updated is False
    assert sftp.files[manifest_path] == previous_manifest
    assert not any(
        operation[0] == "rename" and operation[2] == manifest_path
        for operation in sftp.operations
    )


@pytest.mark.asyncio
async def test_execute_does_not_traverse_a_local_file_symlink(tmp_path: Path) -> None:
    root = tmp_path / "project"
    root.mkdir()
    outside = tmp_path / "secret.txt"
    outside.write_text("secret", encoding="utf-8")
    (root / "link.txt").symlink_to(outside)
    record = project_record(root)
    sftp = FakeSftp()
    service = ProjectSyncService(FakeSshClient(sftp))

    result = await service.execute(
        record,
        SyncPlan(upload=["link.txt"], conflicts=[]),
    )

    assert result.uploaded == []
    assert result.failed == ["link.txt"]
    assert result.manifest_updated is False
    assert not any(operation[0] == "write" for operation in sftp.operations)
