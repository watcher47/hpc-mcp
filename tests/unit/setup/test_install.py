import hashlib
import json
import subprocess
from pathlib import Path
from types import SimpleNamespace
from xml.etree import ElementTree as ET

import pytest

from hpc_mcp.errors import HpcMcpError


def profile(tmp_path):
    path = tmp_path / "servers.json"
    path.write_text(json.dumps({"cluster": {"host": "compute.invalid", "username": "fixture", "password": "fixture-value"}}))
    return path


class Registration:
    def __init__(self):
        self.value = None
        self.calls = []
    def get(self):
        return self.value
    def set(self, command):
        self.calls.append(command)
        self.value = command
    def remove(self):
        self.calls.append(None)
        self.value = None


class Startup:
    def __init__(self):
        self.value = None
    def install(self, command, *, allowed):
        self.verify(allowed)
        self.value = command
    def uninstall(self, *, allowed):
        self.verify(allowed)
        self.value = None
    def verify(self, commands):
        if self.value not in commands:
            raise HpcMcpError("STARTUP_CONFLICT", "Startup was modified")
        return self.value is not None


def service(tmp_path, monkeypatch):
    from hpc_mcp.setup.install import Installer
    package = tmp_path / "package"
    package.mkdir()
    skill = package / "SKILL.md"
    skill.write_text("---\nname: hpc-slurm-workflow\n---\nGeneric workflow")
    registration, startup = Registration(), Startup()
    instance = Installer(profile(tmp_path), tmp_path / "state", tmp_path / "skills",
                         registration=registration, startup=startup)
    monkeypatch.setattr(instance, "preflight", lambda: {"ready": True})
    monkeypatch.setattr(instance, "prepare_release", lambda **kwargs: (package, skill))
    return instance, registration, startup


def test_install_dry_run_is_read_only_and_apply_is_idempotent(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    result = instance.install()
    assert result["apply"] is False
    assert not instance.state.exists()
    assert not registration.calls and startup.value is None
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert len(registration.calls) == 1
    assert startup.value[-1] == "run"
    assert len(list((tmp_path / "skills").rglob("SKILL.md"))) == 1
    metadata = json.loads((instance.state / "installation.json").read_text())
    assert metadata["managed_command"] == registration.value
    assert "fixture-value" not in json.dumps(metadata)
    assert "fixture-value" not in json.dumps(result)


def test_uninstall_preserves_data(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.state.mkdir()
    db = instance.state / "agent.sqlite3"
    db.write_bytes(b"durable watches")
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    instance.uninstall()
    assert startup.value is not None
    instance.uninstall(apply=True)
    instance.uninstall(apply=True)
    assert registration.value is None
    assert db.read_bytes() == b"durable watches"
    assert instance.config.exists()
    assert not list((tmp_path / "skills").rglob("SKILL.md"))


def test_missing_skill_and_failed_smoke_precede_registration(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    monkeypatch.setattr(instance, "prepare_release", lambda **kwargs: (tmp_path, tmp_path / "missing"))
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "SKILL_ASSET_MISSING"
    assert registration.calls == []
    assert startup.value is None


def test_uninstall_refuses_to_remove_registration_replaced_by_user(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    registration.value = ["unrelated-mcp"]
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "INSTALLATION_CHANGED"
    assert registration.value == ["unrelated-mcp"]


def test_scheduler_failure_rolls_back_mcp_and_skill(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    def failed(command, *, allowed):
        raise HpcMcpError("STARTUP_INSTALL_FAILED", "failed")
    monkeypatch.setattr(startup, "install", failed)
    with pytest.raises(HpcMcpError):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert registration.value is None
    assert not list((tmp_path / "skills").rglob("SKILL.md"))


def test_codex_adapter_uses_cli_without_shell_and_rejects_secret_registration():
    from hpc_mcp.setup.install import CodexRegistration
    calls = []
    def runner(args):
        calls.append(args)
        return SimpleNamespace(returncode=0, stdout=json.dumps({"transport": {"type": "stdio", "command": "old", "args": ["arg"], "env": {}}}))
    adapter = CodexRegistration(runner=runner, command=["codex"])
    assert adapter.get() == ["old", "arg"]
    adapter.set([r"C:\tools with space\hpc-mcp.exe", "--config-file", r"C:\local\servers.json"])
    adapter.remove()
    assert calls[1][:5] == ["codex", "mcp", "add", "hpc-mcp", "--"]
    assert calls[2] == ["codex", "mcp", "remove", "hpc-mcp"]
    adapter.runner = lambda args: SimpleNamespace(returncode=0, stdout=json.dumps({"transport": {"type": "stdio", "command": "old", "args": [], "env": {"PRIVATE": "private-value"}}}))
    with pytest.raises(HpcMcpError) as error:
        adapter.get()
    assert "private-value" not in str(error.value)


def test_release_build_and_smoke_complete_before_return(tmp_path):
    from hpc_mcp.setup.install import Installer
    wheel = tmp_path / "hpc_codex_mcp-0.2.0-py3-none-any.whl"
    wheel.write_bytes(b"fixture wheel")
    calls = []
    def runner(args):
        calls.append(args)
        if args[:2] == ["uv", "venv"]:
            scripts = Path(args[-1]) / "Scripts"
            scripts.mkdir(parents=True)
            (scripts / "python.exe").touch()
            (scripts / "pythonw.exe").touch()
        if "-c" in args:
            return SimpleNamespace(returncode=0, stdout=str(tmp_path / "asset"))
        return SimpleNamespace(returncode=0, stdout="")
    installer = Installer(profile(tmp_path), tmp_path / "state", tmp_path / "skills", runner=runner)
    release, asset = installer.prepare_release(wheel=wheel)
    assert (release / wheel.name).read_bytes() == wheel.read_bytes()
    assert [c[:2] for c in calls[:2]] == [["uv", "venv"], ["uv", "pip"]]
    assert any("-c" in call for call in calls)
    install = next(call for call in calls if call[:2] == ["uv", "pip"])
    assert install[install.index("--link-mode") + 1] == "copy"
    assert sum(call[-1] == "--help" for call in calls) == 3
    assert asset == tmp_path / "asset"


@pytest.mark.parametrize("platform,explicit_state", [("nt", False), ("nt", True), ("posix", False), ("posix", True)])
def test_release_location_preserves_state_and_full_digest(tmp_path, monkeypatch, platform, explicit_state):
    from hpc_mcp.setup import install
    state = tmp_path / "user-data"
    home = tmp_path / "home"
    wheel = tmp_path / "hpc_codex_mcp-0.2.0-py3-none-any.whl"
    wheel.write_bytes(b"fixture wheel")
    calls = []

    def runner(args):
        calls.append(args)
        if args[:2] == ["uv", "venv"]:
            Path(args[-1]).mkdir()
        return SimpleNamespace(returncode=0, stdout=str(tmp_path / "asset") if "-c" in args else "")

    monkeypatch.setattr(install.AppPaths, "for_user", lambda: SimpleNamespace(data_directory=state))
    monkeypatch.setattr(install, "resolve_servers_path", lambda config: config)
    monkeypatch.setattr(Path, "home", lambda: home)
    # Isolate platform selection without changing pathlib's native path class.
    with monkeypatch.context() as selection:
        selection.setattr(install, "os", SimpleNamespace(name=platform, environ=install.os.environ))
        instance = install.Installer(profile(tmp_path), state if explicit_state else None,
                                     tmp_path / "skills", runner=runner)
    release, _ = instance.prepare_release(wheel=wheel)
    root = home / ".hpc-codex-mcp" / "releases" if platform == "nt" and not explicit_state else state / "releases"
    assert release == root / hashlib.sha256(wheel.read_bytes()).hexdigest()
    assert instance.state == state
    assert instance.metadata == state / "installation.json"
    assert instance.pending == state / "pending-installation.json"
    assert not instance.metadata.exists() and not instance.pending.exists()
    assert (release / wheel.name).read_bytes() == wheel.read_bytes()
    assert json.loads((release / "release.json").read_text())["sha256"] == release.name
    calls.clear()
    assert instance.prepare_release(wheel=wheel)[0] == release
    assert not any(call[:2] == ["uv", "venv"] for call in calls)
    assert sum(call[-1] == "--help" for call in calls) == 3
    (release / wheel.name).write_bytes(b"tampered archive")
    calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.prepare_release(wheel=wheel)
    assert error.value.code == "WHEEL_CHANGED"
    assert not calls


@pytest.mark.asyncio
async def test_doctor_checks_generated_schema_and_opt_in_slurm_without_outputting_secrets(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    calls = []
    async def schema(*, route):
        assert route == "queue"
        calls.append("schema")
    async def connection(alias):
        calls.append(alias)
        return {"ready": True, "home_directory": "/private/user", "capabilities": {"sbatch": True, "squeue": True, "sacct": True}}
    monkeypatch.setattr(install, "generate_schema", schema)
    monkeypatch.setattr(install, "build_container", lambda **kwargs: SimpleNamespace(connection=SimpleNamespace(test=connection)))
    instance = install.Installer(profile(tmp_path), tmp_path / "state", tmp_path / "skills")
    result = await instance.doctor()
    assert calls == ["schema"]
    assert result["ssh"] == "not_checked"
    result = await instance.doctor(check_ssh=True)
    assert calls == ["schema", "schema", "cluster"]
    assert "fixture-value" not in json.dumps(result) and "private/user" not in json.dumps(result)


def test_registration_lookup_failure_does_not_mean_absence():
    from hpc_mcp.setup.install import CodexRegistration
    adapter = CodexRegistration(runner=lambda args: SimpleNamespace(returncode=1, stdout="private error"))
    with pytest.raises(HpcMcpError) as error:
        adapter.get()
    assert "private error" not in str(error.value)
    calls = iter([SimpleNamespace(returncode=1, stdout=""), SimpleNamespace(returncode=0, stdout="[]")])
    adapter.runner = lambda args: next(calls)
    assert adapter.get() is None


def test_pending_journal_survives_crash_and_is_recovered_on_next_install(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    real_write = install._atomic_json
    def crash(path, value):
        if path == instance.metadata:
            raise KeyboardInterrupt()
        real_write(path, value)
    monkeypatch.setattr(install, "_atomic_json", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert (instance.state / "pending-installation.json").exists()
    monkeypatch.setattr(install, "_atomic_json", real_write)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    instance.uninstall(apply=True)
    assert registration.value is None


def test_previous_registration_containing_profile_secret_cannot_be_persisted(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    registration.value = ["old", "--credential=fixture-value"]
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert not instance.metadata.exists()


def test_external_old_wheel_is_backed_up_for_rollback(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    old = tmp_path / "hpc_codex_mcp-0.1.0-py3-none-any.whl"
    import zipfile
    with zipfile.ZipFile(old, "w") as archive:
        archive.writestr("hpc_codex_mcp-0.1.0.dist-info/METADATA", "Metadata-Version: 2.1\nName: hpc-codex-mcp\nVersion: 0.1.0\n")
    instance.install(apply=True, wheel=tmp_path / "new.whl", rollback_wheel=old)
    metadata = json.loads(instance.metadata.read_text())
    backup = Path(metadata["rollback_wheel"]["path"])
    assert backup != old and backup.read_bytes() == old.read_bytes()
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert json.loads(instance.metadata.read_text())["rollback_wheel"]["path"] == str(backup)
    instance.uninstall(apply=True)
    assert backup.exists() and old.exists()


def test_user_edited_skill_is_never_removed(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    skill = instance.skills / "hpc-slurm-workflow" / "SKILL.md"
    skill.write_text("user edits")
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "SKILL_CONFLICT"
    assert skill.read_text() == "user edits"


def test_failed_smoke_never_registers(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    def fail(**kwargs):
        raise HpcMcpError("SETUP_COMMAND_FAILED", "smoke failed")
    monkeypatch.setattr(instance, "prepare_release", fail)
    with pytest.raises(HpcMcpError):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert registration.calls == [] and startup.value is None


@pytest.mark.parametrize("legacy", [
    ["old", "--token", "unrelated-sensitive-value"],
    ["old", "--password=unrelated-sensitive-value"],
    ["old", "--passphrase", "unrelated-sensitive-value"],
    ["old", "--private-key", "unrelated-sensitive-value"],
    ["https://user:unrelated-sensitive-value@example.invalid/mcp"],
    ["old", "--unknown", "unrelated-sensitive-value"],
    ["old"],
])
def test_unmanaged_registration_rejected_before_staging(tmp_path, monkeypatch, legacy):
    instance, registration, startup = service(tmp_path, monkeypatch)
    registration.value = legacy
    staged = []
    monkeypatch.setattr(instance, "prepare_release", lambda **kw: staged.append(kw))
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert "unrelated-sensitive-value" not in str(error.value)
    assert not staged and not registration.calls and startup.value is None
    assert not list(instance.state.glob("*.json"))
    assert not instance.skills.exists()


def upgrade(instance, tmp_path, monkeypatch):
    package = tmp_path / "package-v2"
    package.mkdir()
    skill = package / "SKILL.md"
    skill.write_text("Complete new skill")
    monkeypatch.setattr(instance, "prepare_release", lambda **kw: (package, skill))
    return skill


def test_managed_upgrade_restores_only_verified_previous_command(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    previous = list(registration.value)
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert registration.value != previous
    instance.uninstall(apply=True)
    assert registration.value == previous


def test_tampered_managed_fingerprint_cannot_authorize_backup(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    record = json.loads(instance.metadata.read_text())
    registration.value = record["managed_command"] = ["old", "--token", "unrelated-sensitive-value"]
    instance.metadata.write_text(json.dumps(record))
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert not (instance.state / "pending-installation.json").exists()


def test_skill_publication_is_atomic_and_crash_recoverable(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    destination = instance.skills / "hpc-slurm-workflow" / "SKILL.md"
    original = destination.read_bytes()
    asset = upgrade(instance, tmp_path, monkeypatch)
    replace = install.os.replace
    observed = []
    def crash(source, target):
        if Path(target) == destination:
            observed.append(Path(source))
            assert Path(source).parent == destination.parent
            assert Path(source).read_bytes() == asset.read_bytes()
            assert destination.read_bytes() == original
            raise KeyboardInterrupt()
        return replace(source, target)
    monkeypatch.setattr(install.os, "replace", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert observed and destination.read_bytes() == original
    pending = json.loads((instance.state / "pending-installation.json").read_text())
    assert pending["phase"] == "skill-publishing"
    assert "unrelated-sensitive-value" not in json.dumps(pending)
    monkeypatch.setattr(install.os, "replace", replace)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert destination.read_bytes() == asset.read_bytes()
    assert not list(destination.parent.glob("*.tmp"))


@pytest.mark.parametrize("failure", ["registration", "startup", "skill"])
def test_uninstall_compensates_each_failure(tmp_path, monkeypatch, failure):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    original_command, original_startup = registration.value, startup.value
    original_metadata = instance.metadata.read_bytes()
    destination = instance.skills / "hpc-slurm-workflow" / "SKILL.md"
    original_skill = destination.read_bytes()
    calls = []
    def fail(*, allowed=None):
        calls.append(failure)
        raise OSError("fixture failure")
    if failure == "registration":
        monkeypatch.setattr(registration, "remove", fail)
    elif failure == "startup":
        monkeypatch.setattr(startup, "uninstall", fail)
    else:
        unlink = Path.unlink
        def fail_skill(path, *args, **kw):
            if path == destination:
                fail()
            return unlink(path, *args, **kw)
        monkeypatch.setattr(Path, "unlink", fail_skill)
    with pytest.raises(HpcMcpError):
        instance.uninstall(apply=True)
    assert calls
    assert registration.value == original_command and startup.value == original_startup
    assert destination.read_bytes() == original_skill
    assert instance.metadata.read_bytes() == original_metadata


def test_uninstall_crash_and_failed_compensation_are_recovered(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    managed = registration.value
    uninstall = startup.uninstall
    def crash(*, allowed):
        raise KeyboardInterrupt()
    monkeypatch.setattr(startup, "uninstall", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.uninstall(apply=True)
    assert (instance.state / "pending-installation.json").exists()
    monkeypatch.setattr(startup, "uninstall", uninstall)
    original_set = registration.set
    monkeypatch.setattr(registration, "set", lambda cmd: (_ for _ in ()).throw(OSError("restore failed")))
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert (instance.state / "pending-installation.json").exists()
    monkeypatch.setattr(registration, "set", original_set)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert registration.value == managed and startup.value is not None
    assert not (instance.state / "pending-installation.json").exists()


def test_uninstall_preflights_startup_ownership_before_any_effect(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    startup.value = ["foreign"]
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "STARTUP_CONFLICT"
    assert not registration.calls and startup.value == ["foreign"]


def test_invalid_rollback_wheel_is_not_archived(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    wheel = tmp_path / "hpc_codex_mcp-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"not a wheel")
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl", rollback_wheel=wheel)
    assert error.value.code == "WHEEL_INVALID"
    assert not (instance.state / "rollback").exists() and not registration.calls


def test_install_compensation_failure_recovers_prior_version_not_candidate(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    atomic = install._atomic_json
    def fail_commit(path, record):
        if path == instance.metadata and record != old:
            raise OSError("commit failed")
        atomic(path, record)
    monkeypatch.setattr(install, "_atomic_json", fail_commit)
    set_registration = registration.set
    def fail_restore(command):
        if command == old["managed_command"]:
            raise OSError("restore failed")
        set_registration(command)
    monkeypatch.setattr(registration, "set", fail_restore)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    journal = json.loads(instance.pending.read_text())
    assert journal["phase"] == "compensating"
    monkeypatch.setattr(install, "_atomic_json", atomic)
    monkeypatch.setattr(registration, "set", set_registration)
    instance._recover_pending()
    assert json.loads(instance.metadata.read_text()) == old
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]


def test_uninstall_compensation_runs_in_reverse_and_continues_after_failure(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    managed = registration.value
    events = []
    remove = registration.remove
    monkeypatch.setattr(registration, "remove", lambda: (events.append("remove-registration"), remove()))
    stop = startup.uninstall
    monkeypatch.setattr(startup, "uninstall", lambda *, allowed: (events.append("remove-startup"), stop(allowed=allowed)))
    unlink = Path.unlink
    def fail_unlink(path, *args, **kwargs):
        if path == instance.destination:
            events.append("remove-skill")
            raise OSError("remove failed")
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", fail_unlink)
    monkeypatch.setattr(instance, "_publish", lambda *args: (events.append("restore-skill"), (_ for _ in ()).throw(OSError("restore failed"))))
    start = startup.install
    monkeypatch.setattr(startup, "install", lambda cmd, *, allowed: (events.append("restore-startup"), start(cmd, allowed=allowed)))
    register = registration.set
    monkeypatch.setattr(registration, "set", lambda cmd: (events.append("restore-registration"), register(cmd)))
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert events == ["remove-registration", "remove-startup", "remove-skill", "restore-skill", "restore-startup", "restore-registration"]
    assert instance.pending.exists() and registration.value == managed and startup.value


def test_interrupted_publication_preserves_user_edit(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old_command, old_startup = registration.value, startup.value
    upgrade(instance, tmp_path, monkeypatch)
    replace = install.os.replace
    def crash(source, target):
        if Path(target) == instance.destination:
            raise KeyboardInterrupt()
        return replace(source, target)
    monkeypatch.setattr(install.os, "replace", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    instance.destination.write_text("user edits after crash")
    monkeypatch.setattr(install.os, "replace", replace)
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert "SKILL_CONFLICT" in str(error.value)
    assert registration.value == old_command and startup.value == old_startup
    assert instance.destination.read_text() == "user edits after crash"
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"


def test_abandoned_partial_skill_temp_is_replaced_after_crash(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    fsync = install.os.fsync
    def crash(descriptor):
        # Only interrupt the skill temporary; journal writes have already closed.
        if list((instance.skills / "hpc-slurm-workflow").glob("*.tmp")):
            raise KeyboardInterrupt()
        fsync(descriptor)
    monkeypatch.setattr(install.os, "fsync", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    temporary = next((instance.skills / "hpc-slurm-workflow").glob("*.tmp"))
    temporary.write_bytes(b"partial")
    monkeypatch.setattr(install.os, "fsync", fsync)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert not temporary.exists()
    assert instance.destination.read_bytes() == (tmp_path / "package" / "SKILL.md").read_bytes()


def test_uninstall_restore_of_prior_managed_registration_failure_is_compensated(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    prior = registration.value
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = registration.value
    setter = registration.set
    def fail(command):
        if command == prior:
            raise OSError("prior version failed")
        setter(command)
    monkeypatch.setattr(registration, "set", fail)
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "UNINSTALLATION_FAILED"
    assert registration.value == current and startup.value and instance.destination.exists()


def test_uninstall_crash_after_metadata_move_recovers_on_next_invocation(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    record = json.loads(instance.metadata.read_text())
    unlink = Path.unlink
    def crash(path, *args, **kwargs):
        if path == instance.pending:
            raise KeyboardInterrupt()
        return unlink(path, *args, **kwargs)
    monkeypatch.setattr(Path, "unlink", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.uninstall(apply=True)
    assert not instance.metadata.exists() and instance.pending.exists()
    monkeypatch.setattr(Path, "unlink", unlink)
    instance._recover_pending()
    assert json.loads(instance.metadata.read_text()) == record
    assert registration.value == record["managed_command"] and startup.value == record["agent_command"]


def test_failed_upgrade_records_compensation_before_unavailable_scheduler_query(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    install, verify = startup.install, startup.verify
    unavailable = False
    observed_phases = []

    def fail_install(command, *, allowed):
        nonlocal unavailable
        if command != old["agent_command"]:
            startup.value = command
            unavailable = True
            raise HpcMcpError("STARTUP_INSTALL_FAILED", "fixture failure")
        install(command, allowed=allowed)

    def query(commands):
        if unavailable:
            observed_phases.append(json.loads(instance.pending.read_text())["phase"])
            raise HpcMcpError("STARTUP_QUERY_FAILED", "fixture query failure")
        return verify(commands)

    monkeypatch.setattr(startup, "install", fail_install)
    monkeypatch.setattr(startup, "verify", query)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert observed_phases and set(observed_phases) == {"compensating"}
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert registration.value == old["managed_command"]
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    unavailable = False
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


@pytest.mark.parametrize("operation", ["install", "uninstall"])
def test_scheduler_edit_after_preflight_preserves_skill_and_compensates(tmp_path, monkeypatch, operation):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    if operation == "install":
        upgrade(instance, tmp_path, monkeypatch)
        install = startup.install

        def edit(command, *, allowed):
            install(command, allowed=allowed)
            instance.destination.write_text("user edit during scheduler action")

        monkeypatch.setattr(startup, "install", edit)
    else:
        uninstall = startup.uninstall

        def edit(*, allowed):
            uninstall(allowed=allowed)
            instance.destination.write_text("user edit during scheduler action")

        monkeypatch.setattr(startup, "uninstall", edit)
    with pytest.raises(HpcMcpError) as error:
        if operation == "install":
            instance.install(apply=True, wheel=tmp_path / "new.whl")
        else:
            instance.uninstall(apply=True)
    assert "SKILL_CONFLICT" in str(error.value)
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert instance.destination.read_text() == "user edit during scheduler action"
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    with pytest.raises(HpcMcpError):
        instance._recover_pending()
    assert instance.destination.read_text() == "user edit during scheduler action"
    assert instance.pending.exists()


@pytest.mark.parametrize("existing", [False, True])
def test_compensation_checks_skill_ownership_at_mutation(tmp_path, monkeypatch, existing):
    instance, registration, startup = service(tmp_path, monkeypatch)
    old = None
    if existing:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        old = json.loads(instance.metadata.read_text())
        upgrade(instance, tmp_path, monkeypatch)
    phase, install = instance._phase, startup.install

    def edit(journal, name):
        phase(journal, name)
        if name == "compensating":
            instance.destination.parent.mkdir(parents=True, exist_ok=True)
            instance.destination.write_text("user edit immediately before compensation")

    def fail(command, *, allowed):
        if old and command == old["agent_command"]:
            return install(command, allowed=allowed)
        raise HpcMcpError("STARTUP_INSTALL_FAILED", "fixture failure")

    monkeypatch.setattr(instance, "_phase", edit)
    monkeypatch.setattr(startup, "install", fail)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert instance.destination.exists()
    assert instance.destination.read_text() == "user edit immediately before compensation"
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert "SKILL_CONFLICT" in str(error.value)
    assert registration.value == (old["managed_command"] if old else None)
    assert startup.value == (old["agent_command"] if old else None)
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"


def test_registration_query_failure_does_not_block_other_compensation(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    install, get = startup.install, registration.get
    unavailable = False

    def fail(command, *, allowed):
        nonlocal unavailable
        install(command, allowed=allowed)
        if command != old["agent_command"]:
            unavailable = True
            raise HpcMcpError("STARTUP_INSTALL_FAILED", "fixture failure")

    def query():
        if unavailable:
            raise HpcMcpError("SETUP_COMMAND_FAILED", "fixture query failure")
        return get()

    monkeypatch.setattr(startup, "install", fail)
    monkeypatch.setattr(registration, "get", query)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    unavailable = False
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert not instance.pending.exists()


@pytest.mark.parametrize("resource", ["registration", "startup"])
def test_compensation_verifies_restoration_before_discarding_journal(tmp_path, monkeypatch, resource):
    instance, registration, startup = service(tmp_path, monkeypatch)

    def fail(command, *, allowed):
        startup.value = command
        raise HpcMcpError("STARTUP_INSTALL_FAILED", "fixture failure")

    monkeypatch.setattr(startup, "install", fail)
    if resource == "registration":
        monkeypatch.setattr(registration, "remove", lambda: None)
    else:
        monkeypatch.setattr(startup, "uninstall", lambda *, allowed: None)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"


def test_interrupted_install_recovers_prior_without_retrying_failed_candidate(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    install, verify = startup.install, startup.verify

    def crash(command, *, allowed):
        raise KeyboardInterrupt()

    monkeypatch.setattr(startup, "install", crash)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    unavailable = False

    def fail(command, *, allowed):
        nonlocal unavailable
        if command != old["agent_command"]:
            unavailable = True
            raise HpcMcpError("STARTUP_INSTALL_FAILED", "fixture failure")
        install(command, allowed=allowed)

    def query(commands):
        if unavailable:
            raise HpcMcpError("STARTUP_QUERY_FAILED", "fixture query failure")
        return verify(commands)

    monkeypatch.setattr(startup, "install", fail)
    monkeypatch.setattr(startup, "verify", query)
    instance._recover_pending()
    assert not unavailable
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert not instance.pending.exists()


def test_compensation_phase_write_failure_retains_prior_recovery_direction(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    atomic, start = install._atomic_json, startup.install

    def fail_start(command, *, allowed):
        assert registration.value != old["managed_command"]
        start(command, allowed=allowed)
        raise OSError("fixture scheduler failure")

    def fail_compensating(path, value):
        if path == instance.pending and value["phase"] == "compensating":
            raise PermissionError("fixture journal failure")
        atomic(path, value)

    monkeypatch.setattr(startup, "install", fail_start)
    monkeypatch.setattr(install, "_atomic_json", fail_compensating)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert instance.pending.exists()
    monkeypatch.setattr(startup, "install", start)
    monkeypatch.setattr(install, "_atomic_json", atomic)
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


def test_atomic_json_retry_exhaustion_retains_durable_compensation_journal(tmp_path, monkeypatch):
    from hpc_mcp.setup import install
    local_os = SimpleNamespace(**vars(install.os))
    local_os.name = "nt"
    monkeypatch.setattr(install, "os", local_os)
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    start, replace = startup.install, install.os.replace
    delays, failed_temps = [], []
    retained = []

    def fail_start(command, *, allowed):
        start(command, allowed=allowed)
        raise OSError("fixture scheduler failure")

    def fail_compensating(source, target):
        if target == instance.pending and json.loads(Path(source).read_text())["phase"] == "compensating":
            failed_temps.append(source)
            retained.append(instance.pending.read_bytes())
            error = PermissionError("fixture journal failure")
            error.winerror = 32
            raise error
        replace(source, target)

    local_os.replace = fail_compensating
    monkeypatch.setattr(install, "time", SimpleNamespace(sleep=delays.append), raising=False)
    monkeypatch.setattr(startup, "install", fail_start)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert len(failed_temps) == 6 and len(set(failed_temps)) == 1
    assert delays == [0.01, 0.02, 0.04, 0.08, 0.16]
    assert instance.pending.read_bytes() == retained[0]
    assert json.loads(retained[0])["phase"] == "startup-installing"
    assert not Path(failed_temps[0]).exists()
    assert json.loads(instance.metadata.read_text()) == old

    local_os.replace = replace
    monkeypatch.setattr(startup, "install", start)
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


@pytest.mark.parametrize("phase", ["registering", "startup-installing", "skill-publishing"])
def test_failed_forward_phase_write_prevents_corresponding_action(tmp_path, monkeypatch, phase):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    atomic, register, start, replace = install._atomic_json, registration.set, startup.install, install.os.replace
    candidate_actions = []

    def fail_phase(path, value):
        if path == instance.pending and value["phase"] == phase:
            raise PermissionError("fixture phase failure")
        atomic(path, value)

    def set_registration(command):
        if command != old["managed_command"]:
            candidate_actions.append("registering")
        register(command)

    def set_startup(command, *, allowed):
        if command != old["agent_command"]:
            candidate_actions.append("startup-installing")
        start(command, allowed=allowed)

    def publish(source, target):
        if Path(target) == instance.destination and hashlib.sha256(Path(source).read_bytes()).hexdigest() != old["skill_sha256"]:
            candidate_actions.append("skill-publishing")
        replace(source, target)

    monkeypatch.setattr(install, "_atomic_json", fail_phase)
    monkeypatch.setattr(registration, "set", set_registration)
    monkeypatch.setattr(startup, "install", set_startup)
    monkeypatch.setattr(install.os, "replace", publish)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_FAILED"
    assert phase not in candidate_actions
    monkeypatch.setattr(install, "_atomic_json", atomic)
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == old


@pytest.mark.parametrize("next_phase", ["startup-installing", "skill-publishing", "skill-published"])
def test_interruption_before_next_phase_restores_prior_installation(tmp_path, monkeypatch, next_phase):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    atomic = install._atomic_json

    def interrupt(path, value):
        if path == instance.pending and value["phase"] == next_phase:
            raise KeyboardInterrupt()
        atomic(path, value)

    monkeypatch.setattr(install, "_atomic_json", interrupt)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert registration.value != old["managed_command"]
    monkeypatch.setattr(install, "_atomic_json", atomic)
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


@pytest.mark.parametrize("phase", ["prepared", "registering", "startup-installing", "skill-publishing", "skill-published"])
def test_existing_journal_resumes_only_untouched_prepared_phase(tmp_path, monkeypatch, phase):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    complete = instance._complete_install

    def interrupt(journal):
        raise KeyboardInterrupt()

    monkeypatch.setattr(instance, "_complete_install", interrupt)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    journal = json.loads(instance.pending.read_text())
    journal["phase"] = phase
    install._atomic_json(instance.pending, journal)
    monkeypatch.setattr(instance, "_complete_install", complete)
    instance._recover_pending()
    expected = journal["target"] if phase == "prepared" else old
    assert registration.value == expected["managed_command"]
    assert startup.value == expected["agent_command"]
    assert instance._skill_hash() == expected["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == expected
    assert not instance.pending.exists()


@pytest.mark.parametrize("failure", ["phase", "metadata", "pending-unlink"])
def test_recovery_filesystem_failure_is_normalized_and_retains_journal(tmp_path, monkeypatch, failure):
    from hpc_mcp.setup import install
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    start, atomic, unlink = startup.install, install._atomic_json, Path.unlink

    def interrupt(command, *, allowed):
        raise KeyboardInterrupt()

    monkeypatch.setattr(startup, "install", interrupt)
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    monkeypatch.setattr(startup, "install", start)
    journal = json.loads(instance.pending.read_text())
    journal["phase"] = "compensating"
    atomic(instance.pending, journal)

    def fail_write(path, value):
        if path == (instance.pending if failure == "phase" else instance.metadata):
            raise PermissionError("fixture journal failure")
        atomic(path, value)

    def fail_unlink(path, *args, **kwargs):
        if path == instance.pending:
            raise PermissionError("fixture cleanup failure")
        return unlink(path, *args, **kwargs)

    if failure == "pending-unlink":
        monkeypatch.setattr(Path, "unlink", fail_unlink)
    else:
        monkeypatch.setattr(install, "_atomic_json", fail_write)
    with pytest.raises(HpcMcpError) as error:
        instance._recover_pending()
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert instance.pending.exists()
    monkeypatch.setattr(install, "_atomic_json", atomic)
    monkeypatch.setattr(Path, "unlink", unlink)
    instance._recover_pending()
    assert registration.value == old["managed_command"]
    assert startup.value == old["agent_command"]
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


@pytest.mark.parametrize("operation,resource,phase", [
    ("install", "registration", "registering"),
    ("uninstall", "registration", "registration-removing"),
    ("install", "startup", "startup-installing"),
    ("uninstall", "startup", "startup-removing"),
])
def test_forward_mutation_preserves_change_after_phase_boundary(tmp_path, monkeypatch, operation, resource, phase):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    if operation == "install":
        upgrade(instance, tmp_path, monkeypatch)
    changed = registration if resource == "registration" else startup
    independent = ["independent-user-command"]
    persist_phase = instance._phase

    def change_after_phase(journal, next_phase):
        persist_phase(journal, next_phase)
        if next_phase == phase:
            changed.value = independent

    monkeypatch.setattr(instance, "_phase", change_after_phase)
    with pytest.raises(HpcMcpError) as error:
        if operation == "install":
            instance.install(apply=True, wheel=tmp_path / "new.whl")
        else:
            instance.uninstall(apply=True)
    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert changed.value == independent
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    assert instance._skill_hash() == old["skill_sha256"]
    if resource == "registration":
        assert startup.value == old["agent_command"]
    else:
        assert registration.value == old["managed_command"]
    # Once the independent conflict is resolved, the durable rollback succeeds.
    changed.value = old["managed_command" if resource == "registration" else "agent_command"]
    instance._recover_pending()
    assert json.loads(instance.metadata.read_text()) == old
    assert not instance.pending.exists()


@pytest.mark.parametrize("skill_before,startup_before", [(True, True), (False, True), (True, False), (False, False)])
def test_upgrade_uninstall_restores_complete_prior_state_and_can_repeat(tmp_path, monkeypatch, skill_before, startup_before):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    if not skill_before:
        instance.destination.unlink()
    if not startup_before:
        startup.value = None
    upgrade(instance, tmp_path, monkeypatch)
    for _ in range(2):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        instance.uninstall(apply=True)
        assert instance.metadata.exists(), "Restoring a managed command also requires its ownership metadata"
        assert json.loads(instance.metadata.read_text()) == old
        assert registration.value == old["managed_command"]
        assert startup.value == (old["agent_command"] if startup_before else None)
        assert instance._skill_hash() == (old["skill_sha256"] if skill_before else None)
    instance.uninstall(apply=True)
    instance.uninstall(apply=True)
    assert registration.value is None and startup.value is None
    assert not instance.metadata.exists() and not instance.destination.exists()


def test_upgrade_uninstall_checks_prior_skill_integrity_before_mutations(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = instance.metadata.read_bytes(), registration.value, startup.value, instance.destination.read_bytes()
    Path(old["skill_source"]).write_text("damaged staged prior asset")
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "SKILL_ASSET_CHANGED"
    assert not registration.calls and not instance.pending.exists()
    assert current == (instance.metadata.read_bytes(), registration.value, startup.value, instance.destination.read_bytes())


def test_format_two_prior_descriptor_cannot_restore_incomplete_installation(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    legacy = json.loads(instance.metadata.read_text())
    legacy["format"] = 2
    legacy["previous"] = {key: old[key] for key in ("release", "config")}
    legacy["fingerprint"] = instance._fingerprint(legacy)
    instance.metadata.write_text(json.dumps(legacy))
    current = instance.metadata.read_bytes(), registration.value, startup.value, instance.destination.read_bytes()
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert not registration.calls and not instance.pending.exists()
    assert current == (instance.metadata.read_bytes(), registration.value, startup.value, instance.destination.read_bytes())


@pytest.mark.parametrize("damage", ["fingerprint", "canonical-command", "depth", "cycle", "presence-type"])
def test_nested_prior_metadata_is_validated_before_any_mutation(tmp_path, monkeypatch, damage):
    import copy
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = json.loads(instance.metadata.read_text())
    assert current["format"] == 3, "Complete prior snapshots need a distinct validated metadata format"
    damaged = copy.deepcopy(current)
    prior = damaged["previous"]["record"]
    if damage == "fingerprint":
        prior["fingerprint"] = "0" * 64
    elif damage == "canonical-command":
        prior["managed_command"] = ["arbitrary-command"]
        prior["fingerprint"] = instance._fingerprint(prior)
    elif damage == "presence-type":
        damaged["previous"]["skill_before"] = "true"
    elif damage == "depth":
        for _ in range(40):
            parent = copy.deepcopy(old)
            parent["previous"] = {"record": prior, "skill_before": True, "startup_before": True}
            parent["fingerprint"] = instance._fingerprint(parent)
            prior = parent
        damaged["previous"]["record"] = prior
    else:
        damaged["previous"]["record"] = damaged
    before = instance.metadata.read_bytes(), registration.value, startup.value, instance.destination.read_bytes()
    registration.calls.clear()
    if damage == "cycle":
        with pytest.raises(HpcMcpError) as error:
            instance._validate_record(damaged)
    else:
        damaged["fingerprint"] = instance._fingerprint(damaged)
        instance.metadata.write_text(json.dumps(damaged))
        with pytest.raises(HpcMcpError) as error:
            instance.uninstall(apply=True)
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert not registration.calls and not instance.pending.exists()
    assert before[1:] == (registration.value, startup.value, instance.destination.read_bytes())


@pytest.mark.parametrize("phase", ["prepared", "startup-installing", "compensating"])
def test_earlier_format_two_upgrade_journal_is_recovered_conservatively(tmp_path, monkeypatch, phase):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    old["format"] = 2
    old["fingerprint"] = instance._fingerprint(old)
    instance.metadata.write_text(json.dumps(old))
    upgrade(instance, tmp_path, monkeypatch)
    complete = instance._complete_install
    monkeypatch.setattr(instance, "_complete_install", lambda journal: (_ for _ in ()).throw(KeyboardInterrupt()))
    with pytest.raises(KeyboardInterrupt):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    journal = json.loads(instance.pending.read_text())
    journal["target"]["format"] = 2
    journal["target"]["previous"] = {key: old[key] for key in ("release", "config")}
    journal["target"]["fingerprint"] = instance._fingerprint(journal["target"])
    journal["phase"] = phase
    if phase != "prepared":
        registration.value = journal["target"]["managed_command"]
    instance.pending.write_text(json.dumps(journal))
    monkeypatch.setattr(instance, "_complete_install", complete)
    instance._recover_pending()
    expected = journal["target"] if phase == "prepared" else old
    assert json.loads(instance.metadata.read_text()) == expected
    assert registration.value == expected["managed_command"]
    assert startup.value == expected["agent_command"]
    assert instance._skill_hash() == expected["skill_sha256"]
    assert not instance.pending.exists()


def test_format_two_complete_installation_can_upgrade_and_be_restored(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    old["format"] = 2
    old["fingerprint"] = instance._fingerprint(old)
    instance.metadata.write_text(json.dumps(old))
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    instance.uninstall(apply=True)
    assert json.loads(instance.metadata.read_text()) == old
    assert startup.value == old["agent_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    instance.uninstall(apply=True)
    assert registration.value is None and startup.value is None


@pytest.mark.parametrize("phase", ["startup-removing", "skill-removing", "metadata-committed"])
def test_upgrade_uninstall_interruption_compensates_complete_current_state(tmp_path, monkeypatch, phase):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    upgrade(instance, tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = json.loads(instance.metadata.read_text())
    persist_phase, unlink = instance._phase, Path.unlink

    def interrupt_phase(journal, next_phase):
        persist_phase(journal, next_phase)
        if next_phase == phase:
            raise KeyboardInterrupt()

    def interrupt_commit(path, *args, **kwargs):
        if path == instance.pending:
            raise KeyboardInterrupt()
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(instance, "_phase", interrupt_phase)
    if phase == "metadata-committed":
        monkeypatch.setattr(Path, "unlink", interrupt_commit)
    with pytest.raises(KeyboardInterrupt):
        instance.uninstall(apply=True)
    monkeypatch.setattr(instance, "_phase", persist_phase)
    monkeypatch.setattr(Path, "unlink", unlink)
    instance._recover_pending()
    assert json.loads(instance.metadata.read_text()) == current
    assert registration.value == current["managed_command"]
    assert startup.value == current["agent_command"]
    assert instance._skill_hash() == current["skill_sha256"]
    assert not instance.pending.exists()


@pytest.mark.parametrize("kind", ["metadata", "journal"])
def test_excessively_nested_json_is_rejected_without_touching_integrations(tmp_path, monkeypatch, kind):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = registration.value, startup.value, instance.destination.read_bytes()
    path = instance.metadata if kind == "metadata" else instance.pending
    path.write_text('[' * 2000 + '0' + ']' * 2000)
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == ("LEGACY_REGISTRATION_UNSAFE" if kind == "metadata" else "INSTALLATION_INTERRUPTED")
    assert path.exists() and not registration.calls
    assert current == (registration.value, startup.value, instance.destination.read_bytes())


def test_null_metadata_is_not_treated_as_an_absent_installation(tmp_path, monkeypatch):
    instance, registration, startup = service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    current = registration.value, startup.value, instance.destination.read_bytes()
    instance.metadata.write_text("null")
    registration.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        instance.uninstall(apply=True)
    assert error.value.code == "LEGACY_REGISTRATION_UNSAFE"
    assert not registration.calls and not instance.pending.exists()
    assert current == (registration.value, startup.value, instance.destination.read_bytes())


class SchedulerRunner:
    """In-memory scheduler boundary for the real WindowsStartup adapter."""
    def __init__(self):
        self.xml = ""
        self.calls = []
        self.before_query = lambda: None

    def __call__(self, args):
        assert args[0] == "schtasks"
        self.calls.append(args)
        if "/Query" in args:
            assert "/TN" in args and "/XML" in args
            self.before_query()
            return SimpleNamespace(returncode=0, stdout=self.xml)
        if "/Create" in args:
            self.xml = Path(args[args.index("/XML") + 1]).read_text(encoding="utf-16")
        elif "/Delete" in args:
            self.xml = ""
        else:
            raise AssertionError(f"Unexpected scheduler action: {args}")
        return SimpleNamespace(returncode=0, stdout="")

    def replace_command(self, command):
        from hpc_mcp.setup.windows import NS
        if command is None:
            self.xml = ""
            return
        root = ET.fromstring(self.xml)
        root.find(f".//{{{NS}}}Command").text = command[0]
        root.find(f".//{{{NS}}}Arguments").text = subprocess.list2cmdline(command[1:])
        self.xml = ET.tostring(root, encoding="unicode")

    @property
    def mutations(self):
        return [args for args in self.calls if "/Create" in args or "/Delete" in args]


def windows_service(tmp_path, monkeypatch):
    from hpc_mcp.setup.windows import WindowsStartup
    instance, registration, _ = service(tmp_path, monkeypatch)
    scheduler = SchedulerRunner()
    instance.startup = WindowsStartup("S-1-5-21-fixture", runner=scheduler, platform="nt")
    return instance, registration, scheduler


def replace_at_final_query(instance, scheduler, phase, command):
    """First phase query is Installer.verify; the second is the adapter's read."""
    observations = []
    def query():
        if instance.pending.exists() and json.loads(instance.pending.read_text())["phase"] == phase:
            observations.append(scheduler.xml)
            if len(observations) == 2:
                scheduler.replace_command(command)
    scheduler.before_query = query
    return observations


@pytest.mark.parametrize("operation", ["upgrade", "uninstall", "uninstall-upgrade"])
def test_startup_final_query_preserves_forward_independent_command(tmp_path, monkeypatch, operation):
    instance, registration, scheduler = windows_service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    if operation != "uninstall":
        upgrade(instance, tmp_path, monkeypatch)
    if operation == "uninstall-upgrade":
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    original_xml = scheduler.xml
    scheduler.calls.clear()
    independent = [r"C:\independent tools\pythonw.exe", "private-independent-argument"]
    phase = "startup-installing" if operation == "upgrade" else "startup-removing"
    observations = replace_at_final_query(instance, scheduler, phase, independent)

    with pytest.raises(HpcMcpError) as error:
        if operation == "upgrade":
            instance.install(apply=True, wheel=tmp_path / "new.whl")
        else:
            instance.uninstall(apply=True)

    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert "private-independent-argument" not in str(error.value)
    assert len(observations) == 2
    assert not scheduler.mutations
    assert instance.startup.verify([independent]) is True
    assert registration.value == old["managed_command"]
    assert instance._skill_hash() == old["skill_sha256"]
    assert json.loads(instance.metadata.read_text()) == old
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    with pytest.raises(HpcMcpError, match="compensation is pending"):
        instance._recover_pending()
    assert not scheduler.mutations and instance.startup.verify([independent])
    # Recovery is possible once the user resolves the independent change.
    scheduler.xml = original_xml
    instance._recover_pending()
    assert not instance.pending.exists()
    assert json.loads(instance.metadata.read_text()) == old


@pytest.mark.parametrize("operation", ["install", "upgrade", "uninstall"])
def test_startup_final_query_preserves_independent_command_during_compensation(tmp_path, monkeypatch, operation):
    instance, registration, scheduler = windows_service(tmp_path, monkeypatch)
    old = None
    if operation != "install":
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        old = json.loads(instance.metadata.read_text())
    if operation == "upgrade":
        upgrade(instance, tmp_path, monkeypatch)
    phase = instance._phase
    def fail_after_action(journal, name):
        phase(journal, name)
        if name == ("skill-removing" if operation == "uninstall" else "skill-published"):
            scheduler.calls.clear()
            # Uninstall's forward action removed the task; simulate a same-owner
            # canonical task appearing before compensation's ownership check.
            if not scheduler.xml:
                instance.startup.install(old["agent_command"])
                scheduler.calls.clear()
            raise OSError("fixture failure after forward scheduler action")
    monkeypatch.setattr(instance, "_phase", fail_after_action)
    independent = [r"C:\independent tools\pythonw.exe", "independent-command"]
    observations = replace_at_final_query(instance, scheduler, "compensating", independent)

    with pytest.raises(HpcMcpError) as error:
        if operation == "uninstall":
            instance.uninstall(apply=True)
        else:
            instance.install(apply=True, wheel=tmp_path / "new.whl")

    assert error.value.code == "INSTALLATION_RECOVERY_FAILED"
    assert len(observations) >= 2
    assert not scheduler.mutations
    assert instance.startup.verify([independent]) is True
    assert registration.value == (old["managed_command"] if old else None)
    assert instance._skill_hash() == (old["skill_sha256"] if old else None)
    assert json.loads(instance.pending.read_text())["phase"] == "compensating"
    assert (json.loads(instance.metadata.read_text()) if instance.metadata.exists() else None) == old
    with pytest.raises(HpcMcpError, match="compensation is pending"):
        instance._recover_pending()
    assert not scheduler.mutations and instance.startup.verify([independent])


@pytest.mark.parametrize("operation", ["upgrade", "uninstall"])
def test_startup_final_query_target_already_applied_is_idempotent(tmp_path, monkeypatch, operation):
    instance, registration, scheduler = windows_service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    target = None
    if operation == "upgrade":
        asset = upgrade(instance, tmp_path, monkeypatch)
        target = instance._commands(asset.parent, instance.config)[1]
    scheduler.calls.clear()
    phase = "startup-installing" if operation == "upgrade" else "startup-removing"
    observations = replace_at_final_query(instance, scheduler, phase, target)
    if operation == "upgrade":
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    else:
        instance.uninstall(apply=True)
    assert len(observations) == 2 and not scheduler.mutations
    assert instance.startup.verify([target]) is (target is not None)
    assert not instance.pending.exists()


@pytest.mark.parametrize("existing", [False, True])
def test_startup_final_query_compensation_already_restored_is_idempotent(tmp_path, monkeypatch, existing):
    instance, registration, scheduler = windows_service(tmp_path, monkeypatch)
    old = None
    if existing:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        old = json.loads(instance.metadata.read_text())
        upgrade(instance, tmp_path, monkeypatch)
    phase = instance._phase
    def fail_after_publication(journal, name):
        phase(journal, name)
        if name == "skill-published":
            scheduler.calls.clear()
            raise OSError("fixture failure after publication")
    monkeypatch.setattr(instance, "_phase", fail_after_publication)
    expected = old["agent_command"] if old else None
    replace_at_final_query(instance, scheduler, "compensating", expected)
    with pytest.raises(HpcMcpError) as error:
        instance.install(apply=True, wheel=tmp_path / "new.whl")
    assert error.value.code == "INSTALLATION_FAILED"
    assert not scheduler.mutations
    assert instance.startup.verify([expected]) is existing
    assert registration.value == (old["managed_command"] if old else None)
    assert instance._skill_hash() == (old["skill_sha256"] if old else None)
    assert not instance.pending.exists()


@pytest.mark.parametrize("operation", ["install", "uninstall"])
@pytest.mark.parametrize("change", ["arguments", "extra-action", "marker", "sid", "absence"])
def test_startup_mutator_validates_final_task_and_permitted_presence(tmp_path, monkeypatch, operation, change):
    from hpc_mcp.setup.windows import NS
    instance, _, scheduler = windows_service(tmp_path, monkeypatch)
    startup = instance.startup
    command = [r"C:\fixture tools\pythonw.exe", "managed-argument"]
    startup.install(command)
    root = ET.fromstring(scheduler.xml)
    if change == "extra-action":
        ET.SubElement(root.find(f".//{{{NS}}}Actions"), f"{{{NS}}}Exec")
    elif change != "absence":
        tag = {"arguments": "Arguments", "marker": "Description", "sid": "UserId"}[change]
        root.find(f".//{{{NS}}}{tag}").text = "independent-value"
    scheduler.xml = "" if change == "absence" else ET.tostring(root, encoding="unicode")
    original = scheduler.xml
    scheduler.calls.clear()
    with pytest.raises(HpcMcpError) as error:
        if operation == "install":
            startup.install(command, allowed=[command])
        else:
            startup.uninstall(allowed=[command])
    assert error.value.code == "STARTUP_CONFLICT"
    assert len(scheduler.calls) == 1 and not scheduler.mutations
    assert scheduler.xml == original


@pytest.mark.parametrize("skill_before,startup_before", [(True, True), (False, True), (True, False), (False, False)])
def test_windows_adapter_upgrade_uninstall_restores_format_three_lifecycle(tmp_path, monkeypatch, skill_before, startup_before):
    instance, registration, scheduler = windows_service(tmp_path, monkeypatch)
    instance.install(apply=True, wheel=tmp_path / "new.whl")
    old = json.loads(instance.metadata.read_text())
    if not skill_before:
        instance.destination.unlink()
    if not startup_before:
        scheduler.xml = ""
    upgrade(instance, tmp_path, monkeypatch)
    for _ in range(2):
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        current = json.loads(instance.metadata.read_text())
        assert current["format"] == 3
        assert current["previous"] == {"record": old, "skill_before": skill_before, "startup_before": startup_before}
        instance.install(apply=True, wheel=tmp_path / "new.whl")
        instance.uninstall(apply=True)
        assert json.loads(instance.metadata.read_text()) == old
        assert registration.value == old["managed_command"]
        assert instance.startup.verify([old["agent_command"] if startup_before else None]) is startup_before
        assert instance._skill_hash() == (old["skill_sha256"] if skill_before else None)
    instance.uninstall(apply=True)
    instance.uninstall(apply=True)
    assert registration.value is None and scheduler.xml == ""
    assert not instance.metadata.exists() and not instance.destination.exists()
