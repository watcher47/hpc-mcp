import hashlib
import os
import posixpath
import stat
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import asyncssh
import pytest

from hpc_mcp import container
from hpc_mcp.config.models import ClusterConfig
from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError
from hpc_mcp.ssh.protocol import CommandResult


class Sftp:
    def __init__(self):
        self.files = {"/allowed/input": b"data"}
        self.directories = {"/", "/allowed", "/allowed/sub"}
        self.redirects = {}
        self.failure = None

    async def realpath(self, path):
        return self.redirects.get(path, posixpath.normpath(path))

    async def lstat(self, path):
        if self.failure:
            raise self.failure
        if path in self.directories:
            return SimpleNamespace(permissions=stat.S_IFDIR | 0o755, size=0)
        if path in self.files:
            return SimpleNamespace(permissions=stat.S_IFREG | 0o644, size=len(self.files[path]))
        raise asyncssh.SFTPNoSuchFile("missing")

    async def open(self, path, mode, attrs=None):
        if "x" in mode and path in self.files:
            raise FileExistsError()
        store = bytearray(self.files[path] if "r" in mode else b"")
        position = 0
        async def read(size):
            nonlocal position
            data = bytes(store[position:position + size])
            position += len(data)
            return data
        async def write(data):
            store.extend(data)
        async def flush():
            pass
        async def close():
            if "r" not in mode:
                self.files[path] = bytes(store)
        return SimpleNamespace(read=read, write=write, flush=flush, close=close)

    async def makedirs(self, path, exist_ok=True):
        self.directories.add(path)

    async def chmod(self, path, mode):
        pass

    async def posix_rename(self, source, target):
        self.files[target] = self.files.pop(source)

    async def remove(self, path):
        self.files.pop(path, None)


class Ssh:
    def __init__(self, sftp):
        self.remote = sftp
        self.calls = []

    @asynccontextmanager
    async def sftp(self):
        self.calls.append("sftp")
        yield self.remote

    async def run_general(self, cmd_string, directory, timeout_ms):
        self.calls.append((cmd_string, directory, timeout_ms))
        return CommandResult(exit_status=0, stdout="ok", stderr="")


def setup(tmp_path, **updates):
    local = tmp_path / "allowed"
    local.mkdir()
    config = ClusterConfig(alias="default", host="compute.invalid", username="fixture-user",
                           password="fixture-value", privateKey="fixture-key", passphrase="fixture-phrase",
                           allowedLocalPaths=[str(local)], allowedRemotePaths=["/allowed"],
                           **updates)
    configs = ConfigRepository(tmp_path / "profiles.json")
    configs.save_cluster(config)
    ssh = Ssh(Sftp())
    return container.GeneralSshService(configs, lambda alias: ssh), ssh, local


@pytest.mark.asyncio
async def test_server_list_has_only_safe_fields_and_does_not_connect(tmp_path):
    service, ssh, _ = setup(tmp_path)
    result = await service.server_list()
    assert result == [{"alias": "default", "host": "compute.invalid", "port": 22,
                       "username": "fixture-user", "transportMode": "exec"}]
    assert ssh.calls == []


@pytest.mark.asyncio
async def test_execute_validates_before_connect_and_requires_allowed_cwd(tmp_path):
    service, ssh, _ = setup(tmp_path)
    with pytest.raises(HpcMcpError) as error:
        await service.execute("pwd")
    assert error.value.code == "COMMAND_BLOCKED"
    assert ssh.calls == []


@pytest.mark.asyncio
async def test_execute_defaults_to_first_allowed_root_and_caps_timeout(tmp_path):
    service, ssh, _ = setup(tmp_path, whitelist=["pwd"], commandTimeoutMs=1000)
    await service.execute("pwd", timeout_ms=5000)
    assert ssh.calls[-1] == ("pwd", "/allowed", 1000)
    with pytest.raises(HpcMcpError) as error:
        await service.execute("pwd", directory="/elsewhere")
    assert error.value.code == "PATH_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_upload_download_are_streamed_and_hashed(tmp_path):
    service, ssh, local = setup(tmp_path)
    source = local / "source"
    source.write_bytes(b"payload")
    result = await service.upload(str(source), "/allowed/new")
    assert result["size"] == 7
    assert result["sha256"] == hashlib.sha256(b"payload").hexdigest()
    assert ssh.remote.files["/allowed/new"] == b"payload"
    target = local / "new"
    result = await service.download("/allowed/input", str(target))
    assert result["size"] == 4
    assert target.read_bytes() == b"data"


@pytest.mark.asyncio
@pytest.mark.parametrize("remote", ["/outside/file", "/allowed-other/file", "/allowed/../outside",
                                    "relative", "C:/allowed/file", "/allowed\\file", "//allowed/file"])
async def test_remote_path_escape_rejected(tmp_path, remote):
    service, ssh, local = setup(tmp_path)
    source = local / "source"
    source.write_bytes(b"data")
    with pytest.raises(HpcMcpError) as error:
        await service.upload(str(source), remote)
    assert error.value.code == "PATH_NOT_ALLOWED"
    assert set(ssh.remote.files) == {"/allowed/input"}


@pytest.mark.asyncio
@pytest.mark.parametrize("redirect", ["/allowed", "/allowed/sub", "/allowed/input"])
async def test_remote_realpath_escape_rejected(tmp_path, redirect):
    service, ssh, local = setup(tmp_path)
    ssh.remote.redirects[redirect] = "/outside"
    with pytest.raises(HpcMcpError) as error:
        await service.download(redirect, str(local / "new"))
    assert error.value.code == "PATH_NOT_ALLOWED"
    assert not (local / "new").exists()


@pytest.mark.asyncio
async def test_new_remote_file_parent_symlink_escape_rejected(tmp_path):
    service, ssh, local = setup(tmp_path)
    ssh.remote.redirects["/allowed/sub"] = "/outside"
    source = local / "source"
    source.write_bytes(b"data")
    with pytest.raises(HpcMcpError) as error:
        await service.upload(str(source), "/allowed/sub/new")
    assert error.value.code == "PATH_NOT_ALLOWED"


@pytest.mark.asyncio
@pytest.mark.parametrize("relative", ["../outside", "../allowed-other/file"])
async def test_local_boundary_escape_rejected(tmp_path, relative):
    service, ssh, local = setup(tmp_path)
    with pytest.raises(HpcMcpError) as error:
        await service.download("/allowed/input", str(local / relative))
    assert error.value.code == "PATH_NOT_ALLOWED"


@pytest.mark.asyncio
async def test_local_symlink_escape_rejected(tmp_path):
    service, ssh, local = setup(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (local / "link").symlink_to(outside, target_is_directory=True)
    with pytest.raises(HpcMcpError) as error:
        await service.download("/allowed/input", str(local / "link" / "new"))
    assert error.value.code == "PATH_NOT_ALLOWED"


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["C:relative", "\\\\?\\C:\\work\\file", "\\\\.\\NUL", "file:stream"])
async def test_ambiguous_windows_paths_are_rejected(tmp_path, path):
    service, _, _ = setup(tmp_path)
    with pytest.raises(HpcMcpError) as error:
        await service.download("/allowed/input", path)
    assert error.value.code == "PATH_NOT_ALLOWED"


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "nt", reason="Windows case-insensitive path semantics")
async def test_windows_case_and_slashes_are_canonicalized(tmp_path):
    service, _, local = setup(tmp_path)
    target = str(local / "new").upper().replace("\\", "/")
    await service.download("/allowed/input", target)
    assert (local / "new").read_bytes() == b"data"


@pytest.mark.asyncio
async def test_sftp_failure_does_not_leak_exception_details(tmp_path):
    service, ssh, local = setup(tmp_path)
    ssh.remote.failure = asyncssh.SFTPFailure("fixture-value fixture-phrase")
    with pytest.raises(HpcMcpError) as error:
        await service.download("/allowed/input", str(local / "new"))
    assert error.value.code == "REMOTE_IO_ERROR"
    assert "fixture" not in str(error.value)


@pytest.mark.asyncio
async def test_upload_cannot_target_allowed_root_directory(tmp_path):
    service, ssh, local = setup(tmp_path)
    source = local / "source"
    source.write_bytes(b"data")
    with pytest.raises(HpcMcpError) as error:
        await service.upload(str(source), "/allowed")
    assert error.value.code == "REMOTE_NOT_FILE"
    assert set(ssh.remote.files) == {"/allowed/input"}


@pytest.mark.asyncio
async def test_download_cannot_target_allowed_root_directory(tmp_path):
    service, ssh, local = setup(tmp_path)
    with pytest.raises(HpcMcpError) as error:
        await service.download("/allowed/input", str(local))
    assert error.value.code == "LOCAL_NOT_FILE"
    assert ssh.calls == []
