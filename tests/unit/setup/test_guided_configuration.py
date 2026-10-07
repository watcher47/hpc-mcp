import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from hpc_mcp.config.repository import ConfigRepository
from hpc_mcp.errors import HpcMcpError


@pytest.fixture
def helper():
    path = Path(__file__).parents[3] / "scripts/configure_windows.py"
    assert path.is_file(), "guided configuration helper is required"
    spec = importlib.util.spec_from_file_location("guided_configuration", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_create_real_config_preserves_special_password(helper, tmp_path):
    password = 'synthetic $`"\\ 中文\n!'
    path = tmp_path / "config/servers.json"
    helper.configure(path, "researcher", password, str(tmp_path))
    profile = ConfigRepository(path).load_cluster("default")
    assert profile.host == "10.29.114.128"
    assert profile.username == "researcher"
    assert profile.password.get_secret_value() == password
    assert profile.allowed_local_paths == [str(tmp_path.resolve())]
    assert json.loads(path.read_text("utf-8"))["default"]["password"] == password


@pytest.mark.parametrize("username,password", [("", "synthetic"), ("SSH_USERNAME", "synthetic"), ("your_username", "synthetic"), ("a/b", "synthetic"), ("researcher", ""), ("researcher", "REPLACE_WITH_PASSWORD")])
def test_reject_incomplete_identity(helper, tmp_path, username, password):
    with pytest.raises(HpcMcpError):
        helper.configure(tmp_path / "servers.json", username, password, str(tmp_path))
    assert not (tmp_path / "servers.json").exists()


@pytest.mark.parametrize("local", ["relative", "missing", "file"])
def test_local_root_must_be_existing_absolute_directory(helper, tmp_path, local):
    (tmp_path / "file").write_text("fixture")
    root = local if local == "relative" else str(tmp_path / local)
    with pytest.raises(HpcMcpError):
        helper.configure(tmp_path / "servers.json", "researcher", "synthetic", root)


@pytest.mark.parametrize("unsafe", ["OneDrive", "checkout"])
def test_config_location_uses_existing_path_rules(helper, tmp_path, unsafe):
    parent = tmp_path / unsafe
    parent.mkdir()
    if unsafe == "checkout":
        (parent / ".git").mkdir()
    with pytest.raises(HpcMcpError):
        helper.configure(parent / "servers.json", "researcher", "synthetic", str(tmp_path))


def test_cancel_leaves_no_config(helper, tmp_path):
    assert helper.configure(tmp_path / "servers.json", "researcher", None, str(tmp_path)) == "cancelled"
    assert list(tmp_path.iterdir()) == []


def test_explicit_remote_root_controls_general_ssh(helper, tmp_path):
    profile = helper.build_profile("researcher", "synthetic", str(tmp_path), remote_root="/work/researcher/projects")
    assert profile.remote_projects_root == "/work/researcher/projects"
    assert profile.allowed_remote_paths == ["/work/researcher/projects"]


def test_remote_root_required_without_confirmed_site_layout(helper, tmp_path):
    with pytest.raises(HpcMcpError):
        helper.build_profile("researcher", "synthetic", str(tmp_path), host="other.example")


def test_existing_config_untouched(helper, tmp_path):
    path = tmp_path / "servers.json"
    path.write_bytes(b"existing fixture")
    with pytest.raises(HpcMcpError):
        helper.configure(path, "researcher", "synthetic", str(tmp_path))
    assert path.read_bytes() == b"existing fixture"


@pytest.mark.parametrize("selection", ["cli", "env", "default"])
def test_dangling_destination_link_is_preserved(helper, tmp_path, monkeypatch, selection):
    path = tmp_path / "servers.json"
    target = tmp_path / "must-not-create.json"
    try:
        path.symlink_to(target)
    except OSError:
        pytest.skip("Windows symlink creation privilege unavailable")
    selected = path if selection == "cli" else None
    if selection == "env":
        monkeypatch.setenv("HPC_MCP_CONFIG", str(path))
    elif selection == "default":
        monkeypatch.delenv("HPC_MCP_CONFIG", raising=False)
        monkeypatch.setattr("hpc_mcp.config.paths.AppPaths.for_user", lambda: SimpleNamespace(servers_file=path))
    with pytest.raises(HpcMcpError):
        helper.configure(selected, "researcher", "synthetic", str(tmp_path))
    assert path.is_symlink()
    assert not target.exists()


def test_racing_destination_creator_cannot_be_overwritten(helper, tmp_path, monkeypatch):
    path = tmp_path / "servers.json"
    publish = helper.publish_file
    def race(temporary, destination):
        destination.write_bytes(b"racing existing fixture")
        publish(temporary, destination)
    monkeypatch.setattr(helper, "publish_file", race)
    with pytest.raises(HpcMcpError):
        helper.configure(path, "researcher", "synthetic", str(tmp_path))
    assert path.read_bytes() == b"racing existing fixture"


@pytest.mark.parametrize("boundary", ["secure_file", "publish_file"])
def test_failure_leaves_no_partial_secret(helper, tmp_path, monkeypatch, boundary, capsys):
    secret = "synthetic-not-to-be-printed"
    def fail(*args):
        if boundary == "secure_file":
            assert args[0].read_bytes() == b""
        raise OSError(secret)
    monkeypatch.setattr(helper, boundary, fail)
    with pytest.raises(HpcMcpError) as error:
        helper.configure(tmp_path / "servers.json", "researcher", secret, str(tmp_path))
    assert secret not in str(error.value)
    assert secret not in repr(error.value)
    assert secret not in str(capsys.readouterr())
    assert list(tmp_path.iterdir()) == []


def test_gui_unavailable_reports_incomplete_without_exception_text(helper, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helper, "os", SimpleNamespace(name="nt"))
    def unavailable(args):
        raise RuntimeError("synthetic-hidden-credential")
    monkeypatch.setattr(helper, "dialog", unavailable)
    assert helper.main(["--config-file", str(tmp_path / "servers.json"), "--username", "researcher", "--local-root", str(tmp_path)]) == 1
    output = capsys.readouterr()
    assert "incomplete" in output.out
    assert "synthetic-hidden-credential" not in output.out + output.err
    assert not (tmp_path / "servers.json").exists()


def test_gui_cancel_is_not_success(helper, tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(helper, "os", SimpleNamespace(name="nt"))
    monkeypatch.setattr(helper, "dialog", lambda args: "cancelled")
    assert helper.main(["--config-file", str(tmp_path / "servers.json"), "--username", "researcher", "--local-root", str(tmp_path)]) == 2
    assert capsys.readouterr().out.strip() == "cancelled"
    assert not (tmp_path / "servers.json").exists()
