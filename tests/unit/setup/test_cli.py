import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.errors import HpcMcpError

from typer.testing import CliRunner

from hpc_mcp.setup import cli
from hpc_mcp.setup.cli import app


def test_init_config_creates_non_secret_template_at_explicit_path(
    tmp_path: Path,
) -> None:
    path = tmp_path / "hpc-config" / "servers.json"

    result = CliRunner().invoke(app, ["--config-file", str(path), "init-config"])

    assert result.exit_code == 0, result.output
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert set(payload) == {"default"}
    profile = payload["default"]
    assert profile["host"] == "COMPUTE_HOST"
    assert profile["username"] == "SSH_USERNAME"
    assert profile["password"] == "REPLACE_WITH_PASSWORD"
    assert "REPLACE_WITH_PASSWORD" not in result.output


def test_init_config_refuses_to_overwrite_existing_file(tmp_path: Path) -> None:
    path = tmp_path / "servers.json"
    path.write_text("keep-me", encoding="utf-8")

    result = CliRunner().invoke(app, ["--config-file", str(path), "init-config"])

    assert result.exit_code == 1
    assert path.read_text(encoding="utf-8") == "keep-me"
    assert "keep-me" not in result.output


def test_setup_validates_existing_config_with_config_file_override(
    tmp_path: Path,
) -> None:
    path = tmp_path / "servers.json"
    path.write_text(
        json.dumps(
            {
                "default": {
                    "host": "compute.invalid",
                    "username": "fixture-user",
                    "password": "fixture-value",
                }
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(app, ["--config-file", str(path), "validate-config"])

    assert result.exit_code == 0, result.output
    assert "fixture-value" not in result.output
    assert "fixture-user" not in result.output


def test_windows_acl_is_applied_to_created_config(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "servers.json"
    calls: list[list[str]] = []
    monkeypatch.setattr(cli.os, "name", "nt")
    monkeypatch.setenv("USERNAME", "fixture-user")
    monkeypatch.setenv("USERDOMAIN", "FIXTURE")

    def run(arguments: list[str], **options: object) -> SimpleNamespace:
        calls.append(arguments)
        assert options["capture_output"] is True
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli.subprocess, "run", run)

    assert cli._restrict_to_current_user(path) is None
    assert calls == [
        [
            "icacls",
            str(path),
            "/inheritance:r",
            "/grant:r",
            "FIXTURE\\fixture-user:(R,W)",
        ]
    ]


def test_acl_failure_returns_non_sensitive_manual_diagnostic(
    tmp_path: Path, monkeypatch
) -> None:
    path = tmp_path / "servers.json"
    monkeypatch.setattr(cli.os, "name", "nt")
    monkeypatch.setenv("USERNAME", "fixture-user")
    monkeypatch.setenv("USERDOMAIN", "FIXTURE")
    monkeypatch.setattr(
        cli.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1),
    )

    diagnostic = cli._restrict_to_current_user(path)

    assert diagnostic is not None
    assert "fixture-user" not in diagnostic
    assert str(path) not in diagnostic


def test_template_publish_never_overwrites_concurrent_creator(tmp_path, monkeypatch):
    path = tmp_path / "servers.json"
    # Inject a second writer at the publication boundary, after existence checks.
    def competing_writer(operation):
        def publish(source, destination, *args, **kwargs):
            path.write_text("concurrent-owned-config", encoding="utf-8")
            return operation(source, destination, *args, **kwargs)
        return publish

    monkeypatch.setattr(cli.os, "replace", competing_writer(cli.os.replace))
    monkeypatch.setattr(cli.os, "link", competing_writer(cli.os.link))
    with pytest.raises(HpcMcpError) as error:
        cli._write_new_template(path)
    assert error.value.code == "CONFIG_EXISTS"
    assert path.read_text() == "concurrent-owned-config"
    assert list(tmp_path.iterdir()) == [path]
