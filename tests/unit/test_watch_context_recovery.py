import json
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from hpc_mcp.agent import service
from hpc_mcp.agent.store import AgentStore
from hpc_mcp.errors import HpcMcpError


def pending(tmp_path):
    store = AgentStore(tmp_path / "agent.sqlite3")
    watch = store.register_watch(server_alias="s", job_id="5393", project_id="p",
        thread_id="durable-origin", session_id="durable-origin", cwd=str(tmp_path))
    event = store.record_terminal(watch.id, "COMPLETED", now=1)
    return store, watch, event


def recover(store, watch, event, tmp_path):
    return store.recover_watch_context(watch_id=watch.id, event_id=event.id,
        expected_thread_id="durable-origin", thread_id="local-target", session_id="local-session",
        cwd=str(tmp_path), now=10)


def test_never_sent_recovery_preserves_event_job_and_audits_old_destination(tmp_path):
    store, watch, event = pending(tmp_path)
    changed = recover(store, watch, event, tmp_path)
    assert changed.id == watch.id and changed.job_id == "5393"
    assert changed.thread_id == "local-target" and not changed.active
    assert store.events()[0].id == event.id
    assert store.events()[0].send_started_at is None
    audit = store.context_recovery(event.id)
    assert audit["previous_thread_id"] == "durable-origin"
    assert audit["thread_id"] == "local-target"
    assert recover(store, watch, event, tmp_path) == changed
    assert len(store.events()) == 1


@pytest.mark.parametrize("fence", ["lease", "send", "queue", "delivered", "wrong_origin"])
def test_recovery_refuses_any_possible_send_or_changed_identity(tmp_path, fence):
    store, watch, event = pending(tmp_path)
    with store._connect() as db:
        if fence == "lease":
            db.execute("UPDATE terminal_events SET delivery_state='IN_FLIGHT',lease_owner='owner',lease_until=99 WHERE id=?", (event.id,))
        elif fence == "send":
            db.execute("UPDATE terminal_events SET send_started_at=2 WHERE id=?", (event.id,))
        elif fence == "queue":
            db.execute("INSERT INTO queue_handoffs(event_id,message_version,message_sha256,context_sha256,state,intent_at,next_observe_at) VALUES(?,2,'m','c','INTENT',2,2)", (event.id,))
        elif fence == "delivered":
            db.execute("UPDATE terminal_events SET delivery_state='DELIVERED',delivered_at=2 WHERE id=?", (event.id,))
        else:
            db.execute("UPDATE job_watches SET thread_id='different' WHERE id=?", (watch.id,))
    with pytest.raises(HpcMcpError) as error:
        recover(store, watch, event, tmp_path)
    assert error.value.code == "WATCH_RECOVERY_BLOCKED"
    assert store.context_recovery(event.id) is None


async def test_local_context_preflight_requires_exact_native_session_and_cwd(tmp_path):
    class Schema:
        def validate(self, *_args, **_kwargs): pass
    async def loader(**_kwargs): return Schema()
    native = {"id": "target", "sessionId": "session", "cwd": str(tmp_path),
              "turns": [], "status": {"type": "active"}}
    calls = []
    class Client:
        def __init__(self, _schema): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *_args): pass
        async def request(self, method, arguments):
            calls.append((method, arguments))
            return {"thread": native}
    arguments = dict(environ={"CODEX_THREAD_ID": "target", "CODEX_SESSION_ID": "session"},
                     cwd=tmp_path, schema_loader=loader, client_factory=Client)
    await service.verify_local_context(**arguments)
    assert calls == [("thread/read", {"threadId": "target", "includeTurns": False})]
    native["sessionId"] = "wrong"
    with pytest.raises(HpcMcpError): await service.verify_local_context(**arguments)
    native["sessionId"] = "session"
    arguments["cwd"] = tmp_path.parent
    with pytest.raises(HpcMcpError): await service.verify_local_context(**arguments)


def test_exact_native_not_loaded_error_is_classified_without_exposing_message():
    from hpc_mcp.agent.app_server import rpc_failure
    error = rpc_failure("thread/read", {"threadId": "target"},
                        {"code": -32600, "message": "thread not loaded: target"})
    assert error.code == "CODEX_THREAD_UNAVAILABLE"
    assert error.details == {"rpc_method": "thread/read", "rpc_code": -32600}
    other = rpc_failure("thread/read", {"threadId": "target"},
                        {"code": -32600, "message": "private secret"})
    assert other.code == "CODEX_REQUEST_FAILED"
    assert "secret" not in str(other)


def test_cli_unreachable_origin_does_not_register_watch(monkeypatch):
    from hpc_mcp.agent import cli
    calls = []
    async def unavailable(): raise HpcMcpError("CODEX_THREAD_UNAVAILABLE", "Origin is not local")
    monkeypatch.setattr(cli, "verify_local_context", unavailable, raising=False)
    monkeypatch.setattr(cli, "build_container", lambda **_kwargs: SimpleNamespace(
        slurm_watch=SimpleNamespace(register=lambda *_a: calls.append("register"))))
    result = CliRunner().invoke(cli.app, ["watch", "--server", "s", "--job-id", "5393", "--project-id", "p"])
    assert result.exit_code == 1 and "CODEX_THREAD_UNAVAILABLE" in result.output
    assert calls == []
