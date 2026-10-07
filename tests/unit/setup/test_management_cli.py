import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner


@pytest.mark.parametrize("command", ["install", "uninstall"])
def test_management_commands_default_read_only_and_apply_is_explicit(monkeypatch, tmp_path, command):
    from hpc_mcp.setup import cli
    calls = []
    class Fake:
        def __init__(self, config):
            pass
        def install(self, **kwargs):
            calls.append(kwargs)
            return kwargs
        uninstall = install
    monkeypatch.setattr(cli, "Installer", Fake, raising=False)
    runner = CliRunner()
    result = runner.invoke(cli.app, ["--config-file", str(tmp_path / "servers.json"), command])
    assert result.exit_code == 0, result.output
    assert calls[0]["apply"] is False
    assert runner.invoke(cli.app, ["--config-file", str(tmp_path / "servers.json"), command, "--apply"]).exit_code == 0
    assert calls[1]["apply"] is True


def test_doctor_handles_sensitive_errors(monkeypatch, tmp_path):
    from hpc_mcp.setup import cli
    class Fake:
        def __init__(self, config):
            pass
        async def doctor(self, **kwargs):
            raise OSError("private diagnostic")
    monkeypatch.setattr(cli, "Installer", Fake, raising=False)
    result = CliRunner().invoke(cli.app, ["--config-file", str(tmp_path / "servers.json"), "doctor"])
    assert result.exit_code == 1
    assert "SETUP_FAILED" in result.output
    assert "private diagnostic" not in result.output


@pytest.mark.parametrize("command", ["install-startup", "uninstall-startup"])
def test_agent_startup_apply_calls_adapter_only_when_requested(monkeypatch, tmp_path, command):
    from hpc_mcp.agent import cli
    calls = []
    class Fake:
        def install(self, command):
            calls.append(command)
        def uninstall(self):
            calls.append("remove")
    monkeypatch.setattr(cli, "WindowsStartup", Fake, raising=False)
    config = tmp_path / "servers.json"
    result = CliRunner().invoke(cli.app, ["--config-file", str(config), command, "--apply"])
    assert result.exit_code == 0, result.output
    assert len(calls) == 1
