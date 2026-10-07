import os
from pathlib import Path

import pytest


def test_windows_state_is_shared(monkeypatch, tmp_path):
    from hpc_mcp.config import paths
    monkeypatch.setattr(paths, "os", type("OS", (), {"name": "nt"}))
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert paths.AppPaths.for_user().data_directory == tmp_path / ".hpc-codex-mcp" / "state"
    assert paths.AppPaths(tmp_path / "config", tmp_path / "custom").data_directory == tmp_path / "custom"


def test_non_windows_state_uses_platformdirs(monkeypatch, tmp_path):
    from hpc_mcp.config import paths
    monkeypatch.setattr(paths, "os", type("OS", (), {"name": "posix"}))
    monkeypatch.setattr(paths, "user_data_path", lambda name: tmp_path)
    assert paths.AppPaths.for_user().data_directory == tmp_path


@pytest.mark.parametrize("platform", ["nt", "posix"])
def test_path_precedence(monkeypatch, tmp_path, platform):
    from hpc_mcp import codex_executable as cli
    monkeypatch.setattr(cli.shutil, "which", lambda name: "preferred-codex")
    assert cli.resolve_codex_executable(platform=platform, local_app_data=tmp_path) == "preferred-codex"


def test_desktop_newest_and_stable_tie(monkeypatch, tmp_path):
    from hpc_mcp import codex_executable as cli
    monkeypatch.setattr(cli.shutil, "which", lambda name: None)
    for version, timestamp in [("old", 1), ("a", 2), ("b", 2)]:
        path = tmp_path / "OpenAI" / "Codex" / "bin" / version / "codex.exe"
        path.parent.mkdir(parents=True)
        path.touch()
        os.utime(path, (timestamp, timestamp))
    assert cli.resolve_codex_executable(platform="nt", local_app_data=tmp_path) == str(tmp_path / "OpenAI/Codex/bin/a/codex.exe")
    assert cli.resolve_codex_executable(platform="posix", local_app_data=tmp_path) == "codex"


def test_missing_cli(monkeypatch, tmp_path):
    from hpc_mcp import codex_executable as cli
    monkeypatch.setattr(cli.shutil, "which", lambda name: None)
    assert cli.resolve_codex_executable(platform="nt", local_app_data=tmp_path) == "codex"


def test_msix_backing_discovery_is_scoped(monkeypatch, tmp_path):
    from hpc_mcp import codex_executable as cli
    monkeypatch.setattr(cli.shutil, "which", lambda name: None)
    path = tmp_path / "Packages/OpenAI.Codex_fixture/LocalCache/Local/OpenAI/Codex/bin/version/codex.exe"
    path.parent.mkdir(parents=True)
    path.touch()
    foreign = tmp_path / "Packages/Other.App_fixture/LocalCache/Local/OpenAI/Codex/bin/version/codex.exe"
    foreign.parent.mkdir(parents=True)
    foreign.touch()
    os.utime(foreign, (100, 100))
    assert cli.resolve_codex_executable(platform="nt", local_app_data=tmp_path) == str(path)


def test_explicit_app_server_command_preserved(monkeypatch):
    from hpc_mcp.agent import app_server
    monkeypatch.setattr(app_server, "resolve_codex_executable", lambda: pytest.fail("resolved explicit command"))
    assert app_server.AppServerClient(None, command=["custom", "arg"]).command == ["custom", "arg"]


@pytest.mark.asyncio
async def test_default_schema_and_client_use_resolver(monkeypatch):
    from hpc_mcp.agent import app_server
    from hpc_mcp.errors import HpcMcpError
    calls = []
    monkeypatch.setattr(app_server, "resolve_codex_executable", lambda: "desktop-cli")
    async def missing(*args, **kwargs):
        calls.append(args)
        raise FileNotFoundError("private diagnostic")
    monkeypatch.setattr(app_server.asyncio, "create_subprocess_exec", missing)
    assert app_server.AppServerClient(None).command[0] == "desktop-cli"
    with pytest.raises(HpcMcpError) as error:
        await app_server.generate_schema()
    assert calls[0][0] == "desktop-cli"
    assert error.value.code == "CODEX_UNAVAILABLE"
    assert "private diagnostic" not in str(error.value)
    await_command = ["custom", "argument"]
    with pytest.raises(HpcMcpError):
        await app_server.generate_schema(command=await_command)
    assert calls[1][:2] == tuple(await_command)


def test_registration_uses_shared_resolver(monkeypatch):
    from hpc_mcp.setup import install
    from types import SimpleNamespace
    monkeypatch.setattr(install, "resolve_codex_executable", lambda: "desktop-cli")
    calls = []
    def runner(args):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout='{"transport":{"type":"stdio","command":"owned"}}')
    adapter = install.CodexRegistration(runner=runner)
    assert adapter.get() == ["owned"]
    adapter.set(["owned"])
    adapter.remove()
    assert [call[0] for call in calls] == ["desktop-cli"] * 3
    override = install.CodexRegistration(runner=runner, command=["custom", "argument"])
    override.remove()
    assert calls[-1][:2] == ["custom", "argument"]
