import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from hpc_mcp.errors import HpcMcpError
from hpc_mcp.project.registry import ProjectRegistry


@pytest.fixture
def context(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    from hpc_mcp.agent.service import WatchService
    registry = ProjectRegistry(tmp_path / "projects.sqlite3")
    project = registry.ensure(tmp_path, "cluster", "/remote")
    registry.register_job(project.id, "123", "slurm-123.out", "slurm-123.out")
    store = AgentStore(tmp_path / "agent.sqlite3")
    return SimpleNamespace(registry=registry, project=project, store=store,
                           service=WatchService(registry, store))


def test_register_requires_both_terminal_environment_ids(context, tmp_path):
    for env in ({}, {"CODEX_THREAD_ID": "t"}, {"CODEX_SESSION_ID": "s"}):
        with pytest.raises(HpcMcpError) as error:
            context.service.register("cluster", "123", context.project.id, environ=env, cwd=tmp_path)
        assert error.value.code == "CODEX_CONTEXT_MISSING"
    assert context.store.watches() == []


def test_register_checks_job_project_server_and_cwd(context, tmp_path):
    env = {"CODEX_THREAD_ID": "t", "CODEX_SESSION_ID": "s"}
    for alias, job, project_id, cwd in [
        ("other", "123", context.project.id, tmp_path),
        ("cluster", "321", context.project.id, tmp_path),
        ("cluster", "123", "wrong", tmp_path),
        ("cluster", "123", context.project.id, tmp_path.parent),
    ]:
        with pytest.raises(HpcMcpError):
            context.service.register(alias, job, project_id, environ=env, cwd=cwd)
    watch = context.service.register("cluster", "123", context.project.id, environ=env, cwd=tmp_path)
    assert watch.thread_id == "t" and watch.session_id == "s"
    assert watch.cwd == str(tmp_path.resolve())


@pytest.mark.asyncio
async def test_mcp_verification_never_creates_watch(context, tmp_path):
    with pytest.raises(HpcMcpError) as error:
        await context.service.verify("123", "cluster")
    assert error.value.code == "WATCH_NOT_REGISTERED"
    assert not context.store.watches()
    watch = context.service.register("cluster", "123", context.project.id,
        environ={"CODEX_THREAD_ID": "t", "CODEX_SESSION_ID": "s"}, cwd=tmp_path)
    assert (await context.service.verify("123", "cluster"))["watch_id"] == watch.id
    context.store.record_terminal(watch.id, "COMPLETED", now=1)
    assert (await context.service.verify("123", "cluster"))["terminal_event_pending"]


def test_cli_watch_status_retry_and_missing_context(context, monkeypatch):
    from hpc_mcp.agent import cli
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(
        agent_store=context.store, slurm_watch=context.service))
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.delenv("CODEX_SESSION_ID", raising=False)
    runner = CliRunner()
    args = ["watch", "--server", "cluster", "--job-id", "123", "--project-id", context.project.id]
    monkeypatch.chdir(context.project.local_root)
    assert "CODEX_CONTEXT_MISSING" in runner.invoke(cli.app, args).output
    monkeypatch.setenv("CODEX_THREAD_ID", "t")
    monkeypatch.setenv("CODEX_SESSION_ID", "s")
    async def verified():
        return {"thread_id": "t", "session_id": "s", "cwd": str(context.project.local_root)}
    monkeypatch.setattr(cli, "verify_local_context", verified)
    result = runner.invoke(cli.app, args)
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["job_id"] == "123"
    assert runner.invoke(cli.app, args).exit_code == 0
    result = runner.invoke(cli.app, ["status"])
    assert len(json.loads(result.output)["watches"]) == 1
    context.store.server_ready("cluster", "v1", now=0)
    context.store.server_error("cluster", "AUTH_FAILED", now=0, pause=True)
    assert runner.invoke(cli.app, ["retry-server", "--server", "cluster"]).exit_code == 0
    assert not context.store.server_states()[0].paused
    assert runner.invoke(cli.app, args + ["--thread-id", "invented"]).exit_code != 0


def test_config_fingerprint_changes_with_file_bytes(tmp_path):
    from hpc_mcp.agent.cli import config_version
    path = tmp_path / "servers.json"
    with pytest.raises(HpcMcpError):
        config_version(path)
    path.write_text("first")
    first = config_version(path)
    assert config_version(path) == first
    path.write_text("second")
    assert config_version(path) != first


def test_recover_watch_cli_uses_verified_terminal_and_preserves_job_event(context, monkeypatch):
    from hpc_mcp.agent import cli
    watch = context.service.register("cluster", "123", context.project.id,
        environ={"CODEX_THREAD_ID": "old", "CODEX_SESSION_ID": "old-session"}, cwd=context.project.local_root)
    event = context.store.record_terminal(watch.id, "COMPLETED", now=1)
    async def verified():
        return {"thread_id": "local", "session_id": "local-session", "cwd": str(context.project.local_root)}
    monkeypatch.setattr(cli, "verify_local_context", verified)
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(
        agent_store=context.store, slurm_watch=context.service))
    args = ["recover-watch", "--server", "cluster", "--job-id", "123",
            "--project-id", context.project.id, "--event-id", event.id, "--from-thread", "old"]
    runner = CliRunner()
    for _ in range(2):
        result = runner.invoke(cli.app, args)
        assert result.exit_code == 0, result.output
        assert json.loads(result.output)["watch"]["thread_id"] == "local"
    assert len(context.store.events()) == 1 and context.store.events()[0].id == event.id
    assert context.store.events()[0].send_started_at is None
    assert context.store.find_watch("cluster", "123").id == watch.id


def test_recover_watch_refuses_different_project_directory(context, monkeypatch):
    from hpc_mcp.agent import cli
    watch = context.service.register("cluster", "123", context.project.id,
        environ={"CODEX_THREAD_ID": "old", "CODEX_SESSION_ID": "old"}, cwd=context.project.local_root)
    event = context.store.record_terminal(watch.id, "COMPLETED", now=1)
    async def verified():
        return {"thread_id": "local", "session_id": "local", "cwd": str(context.project.local_root.parent)}
    monkeypatch.setattr(cli, "verify_local_context", verified)
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(
        agent_store=context.store, slurm_watch=context.service))
    result = CliRunner().invoke(cli.app, ["recover-watch", "--server", "cluster", "--job-id", "123",
        "--project-id", context.project.id, "--event-id", event.id, "--from-thread", "old"])
    assert result.exit_code == 1 and "WATCH_RECOVERY_BLOCKED" in result.output
    assert context.store.find_watch("cluster", "123").thread_id == "old"


def test_cli_run_wires_real_slurm_adapter_without_model_or_ssh_at_setup(context, monkeypatch, tmp_path):
    from hpc_mcp.agent import cli
    calls = []
    adapter = object()
    config = tmp_path / "servers.json"
    config.write_text("{}")
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(
        agent_store=context.store, slurm_query=adapter, configs=SimpleNamespace(path=config)))
    class OnceMonitor:
        def __init__(self, store, slurm, version, *, after_poll):
            assert store is context.store and slurm is adapter
            assert version() == cli.config_version(config)
            self.after_poll = after_poll
        async def run(self):
            calls.append("poll")
            await self.after_poll()
    class Delivery:
        def __init__(self, store):
            assert store is context.store
        async def deliver_pending(self):
            calls.append("deliver")
        async def aclose(self):
            pass
    monkeypatch.setattr(cli, "Monitor", OnceMonitor)
    monkeypatch.setattr(cli, "DeliveryService", Delivery, raising=False)
    result = CliRunner().invoke(cli.app, ["run"])
    assert result.exit_code == 0, result.output
    assert calls == ["poll", "deliver"]


def test_manual_recovery_cli_only_prints_original_thread_and_message(context, monkeypatch):
    from hpc_mcp.agent import cli
    watch = context.store.register_watch(server_alias="cluster", job_id="123", project_id=context.project.id,
        thread_id="origin", session_id="session", cwd=str(context.project.local_root))
    event = context.store.record_terminal(watch.id, "FAILED", now=1)
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(agent_store=context.store))
    result = CliRunner().invoke(cli.app, ["manual-recovery", "--event-id", event.id])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["thread_id"] == "origin"
    assert context.store.events()[0].delivery_state == "PENDING"


@pytest.mark.parametrize("command", ["install-startup", "uninstall-startup"])
def test_startup_defaults_to_dry_run_without_mutating_user_scheduler(context, monkeypatch, command):
    from hpc_mcp.agent import cli
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: object())
    result = CliRunner().invoke(cli.app, [command])
    assert result.exit_code == 0
    assert '"apply": false' in result.output


def test_cli_storage_failure_does_not_expose_raw_error(monkeypatch):
    from hpc_mcp.agent import cli
    def failed(**kwargs):
        raise OSError("untrusted private diagnostic")
    monkeypatch.setattr(cli, "build_container", failed)
    result = CliRunner().invoke(cli.app, ["status"])
    assert result.exit_code == 1
    assert "AGENT_STORAGE_FAILED" in result.output
    assert "untrusted" not in result.output


def test_cli_diagnostics_bounded_and_status_summary(context, monkeypatch):
    from hpc_mcp.agent import cli
    watch = context.store.register_watch(server_alias="cluster", job_id="123", project_id="p",
        thread_id="t", session_id="s", cwd="/project")
    event = context.store.record_terminal(watch.id, "COMPLETED", now=0)
    context.store.claim_event("owner", now=1)
    context.store.defer_event(event.id, "owner", retry_at=3, now=2, error_code="CODEX_SECRET_PRIVATE")
    monkeypatch.setattr(cli, "build_container", lambda **kwargs: SimpleNamespace(agent_store=context.store))
    runner = CliRunner()
    result = runner.invoke(cli.app, ["diagnostics", "--event-id", event.id, "--limit", "1"])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["returned_count"] == 1
    assert "SECRET_PRIVATE" not in result.output
    status = json.loads(runner.invoke(cli.app, ["status"]).output)
    assert status["events"][0]["diagnostics"]["last_attempt"]["outcome"] == "DEFERRED"
    assert runner.invoke(cli.app, ["diagnostics", "--event-id", event.id, "--limit", "101"]).exit_code != 0
