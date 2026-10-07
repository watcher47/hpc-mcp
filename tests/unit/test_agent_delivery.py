import asyncio
import copy
import json
import gc
import sqlite3
from contextlib import asynccontextmanager

import pytest

from hpc_mcp.agent.store import AgentStore, TERMINAL_STATES
from hpc_mcp.errors import HpcMcpError
from tests.unit.test_app_server import contract


@pytest.mark.asyncio
async def test_background_batch_error_is_observed_and_safely_diagnosed(tmp_path, monkeypatch, caplog):
    store, _, _ = setup_event(tmp_path)
    def unavailable(*args, **kwargs):
        raise sqlite3.OperationalError("untrusted private database detail")
    monkeypatch.setattr(store, "claim_event", unavailable)
    service = delivery(store, FakeClient())
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    unobserved = []
    loop.set_exception_handler(lambda loop, context: unobserved.append(context))
    try:
        await service.deliver_pending()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        # Replacing a completed batch used to lose its unobserved exception.
        await service.deliver_pending()
        await service.aclose()
        gc.collect()
        await asyncio.sleep(0)
        assert not unobserved
        assert "CODEX_BACKGROUND_FAILED" in caplog.text
        assert "untrusted private" not in caplog.text
    finally:
        loop.set_exception_handler(previous_handler)


def setup_event(tmp_path, state="COMPLETED", job="123"):
    store = AgentStore(tmp_path / "agent.sqlite3")
    watch = store.register_watch(server_alias="research", job_id=job, project_id="project-id",
        thread_id="original-thread", session_id="original-session", cwd=str(tmp_path))
    return store, watch, store.record_terminal(watch.id, state, now=1)


class FakeClient:
    def __init__(self, status="idle", history=None):
        self.calls = []
        self.thread = {"id": "original-thread", "sessionId": "original-session",
            "status": {"type": status}, "turns": history or [], "historyMode": "legacy"}
        self.error = None
        self.on_start = None
        self.on_resume = None
        self.pages = []
        # Independent persisted observation; synthetic ID exists only in tests.
        self.latest = {"id": "prior-completed", "status": "completed",
                       "items": [], "itemsView": "notLoaded"}
        self.latest_pages = []

    async def wait_turn_completed(self, thread_id, turn_id):
        return "COMPLETED"

    async def request(self, method, params):
        self.calls.append((method, params))
        if method == "thread/read":
            thread = copy.deepcopy(self.thread)
            if not params["includeTurns"]:
                thread["turns"] = []
            return {"thread": thread}
        if method == "thread/resume":
            if self.on_resume:
                self.on_resume()
            return {"thread": copy.deepcopy(self.thread)}
        if method == "thread/items/list":
            return self.pages.pop(0)
        if method == "thread/turns/list":
            return (self.latest_pages.pop(0) if self.latest_pages else
                    {"data": [copy.deepcopy(self.latest)], "nextCursor": None})
        if method == "turn/start":
            if self.on_start:
                self.on_start(params)
            if self.error:
                raise self.error
            return {"turn": {"id": "new-turn", "status": "inProgress", "items": []}}
        raise AssertionError(method)


def delivery(store, client, **kwargs):
    from hpc_mcp.agent.delivery import DeliveryService
    @asynccontextmanager
    async def factory(schema):
        yield client
    async def loader():
        return contract()
    return DeliveryService(store, client_factory=factory, schema_loader=kwargs.pop("schema_loader", loader),
                           clock=kwargs.pop("clock", lambda: 100), **kwargs)


def marker_turn(event, watch):
    from hpc_mcp.agent.delivery import completion_message
    return {"items": [{"type": "userMessage", "clientId": event.id,
        "content": [{"type": "text", "text": completion_message(watch, event)}]}]}


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["inProgress", "interrupted", "failed"])
async def test_readiness_rejects_unloaded_with_unfinished_persisted_turn(tmp_path, status):
    store, _, event = setup_event(tmp_path)
    client = FakeClient("notLoaded")
    client.latest["status"] = status
    client.on_resume = lambda: client.thread.update(status={"type": "idle"})
    service = delivery(store, client)
    await service.deliver_once()
    result = store.events()[0]
    assert result.last_error == "CODEX_THREAD_NOT_READY"
    assert result.send_started_at is None
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)
    assert store.diagnostics(event.id)["attempts"][0]["stage"] == "READINESS"
    # Ordinary observations always defer one minute, including later attempts.
    await delivery(store, client, clock=lambda: 160).deliver_once()
    assert store.events()[0].next_retry_at == 220


@pytest.mark.asyncio
@pytest.mark.parametrize("page", [None, {}, {"data": []}, {"data": [None]},
    {"data": [{"id": "", "status": "completed", "items": [], "itemsView": "notLoaded"}]},
    {"data": [{"id": "a", "status": "completed"}]},
    {"data": [{"id": "a", "status": "unknown", "items": [], "itemsView": "notLoaded"}]},
    {"data": [{"id": "a", "status": "completed", "items": [], "itemsView": "summary"}]},
    {"data": [{"id": "a", "status": "completed", "items": [], "itemsView": "notLoaded"}], "nextCursor": 5},
    {"data": [{"id": "a", "status": "completed"}, {"id": "b", "status": "completed"}]}])
async def test_readiness_malformed_latest_page_never_resumes(tmp_path, page):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.latest_pages = [page]
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_READINESS_UNAVAILABLE"
    assert store.events()[0].send_started_at is None
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
async def test_readiness_missing_schema_preserves_marker_reconciliation(tmp_path):
    store, watch, event = setup_event(tmp_path)
    async def loader():
        schema = contract()
        del schema.methods["thread/turns/list"]
        return schema
    client = FakeClient()
    await delivery(store, client, schema_loader=loader).deliver_once()
    assert store.events()[0].last_error == "CODEX_READINESS_UNAVAILABLE"
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)
    # An uncertain send reconciles before requiring readiness support.
    claimed = store.claim_event("old", now=160, lease_seconds=1)
    assert claimed and store.mark_send_started(event.id, "old", now=160)
    client = FakeClient("active", history=[marker_turn(event, watch)])
    await delivery(store, client, schema_loader=loader, clock=lambda: 162).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert not any(m in {"thread/turns/list", "thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["itemsView", "descending"])
async def test_readiness_incompatible_request_shape_defers_before_resume(tmp_path, problem):
    store, _, _ = setup_event(tmp_path)
    async def loader():
        schema = contract()
        properties = schema.methods["thread/turns/list"]["properties"]
        if problem == "itemsView":
            del properties["itemsView"]
        else:
            properties["sortDirection"] = {"enum": ["asc"]}
        return schema
    client = FakeClient()
    await delivery(store, client, schema_loader=loader).deliver_once()
    assert store.events()[0].last_error == "CODEX_READINESS_UNAVAILABLE"
    assert store.events()[0].send_started_at is None
    assert not any(m in {"thread/turns/list", "thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("prior_send,found", [(False, True), (True, False)])
async def test_readiness_unavailable_does_not_mask_reconciliation(tmp_path, prior_send, found):
    store, watch, event = setup_event(tmp_path)
    if prior_send:
        store.claim_event("old", now=1, lease_seconds=1)
        store.mark_send_started(event.id, "old", now=1)
    async def loader():
        schema = contract()
        del schema.methods["thread/turns/list"]
        return schema
    client = FakeClient(history=[marker_turn(event, watch)] if found else [])
    await delivery(store, client, schema_loader=loader).deliver_once()
    result = store.events()[0]
    assert result.delivery_state == ("DELIVERED" if found else "MANUAL_REQUIRED")
    if not found:
        assert result.handoff_error == "CODEX_SEND_OUTCOME_UNKNOWN"
    assert not any(m in {"thread/turns/list", "thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("change_at", ["resume", "second-history"])
async def test_readiness_completed_identity_changes_before_send(tmp_path, change_at):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    if change_at == "resume":
        client.on_resume = lambda: client.latest.update(id="new-completed")
    else:
        original = client.request
        async def request(method, params):
            response = await original(method, params)
            if method == "thread/read" and sum(m == "thread/resume" for m, _ in client.calls):
                client.latest["id"] = "new-completed"
            return response
        client.request = request
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_THREAD_CHANGED"
    assert store.events()[0].send_started_at is None
    assert not any(m == "turn/start" for m, _ in client.calls)
    assert client.calls[-1][0] == "thread/turns/list"
    client.latest["id"] = "another-completed"
    client.calls.clear()
    await delivery(store, client, clock=lambda: 160).deliver_once()
    assert store.events()[0].next_retry_at == 220


@pytest.mark.asyncio
async def test_readiness_final_check_rejects_new_in_progress_turn(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.on_resume = lambda: client.latest.update(status="inProgress")
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_THREAD_NOT_READY"
    assert store.events()[0].send_started_at is None
    assert not any(m == "turn/start" for m, _ in client.calls)


@pytest.mark.asyncio
async def test_readiness_stable_completion_observed_immediately_before_send(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    service = delivery(store, client)
    await service.deliver_once()
    await service.wait_idle()
    assert store.events()[0].delivery_state == "DELIVERED"
    methods = [m for m, _ in client.calls]
    assert methods.count("turn/start") == 1
    assert methods[methods.index("thread/resume") - 1] == "thread/turns/list"
    assert methods[-2:] == ["thread/turns/list", "turn/start"]
    assert all(p == {"threadId": "original-thread", "limit": 1,
        "sortDirection": "desc", "itemsView": "notLoaded"}
        for m, p in client.calls if m == "thread/turns/list")


@pytest.mark.asyncio
async def test_delivery_journal_safe_rpc_and_uncertain_send(tmp_path):
    store, _, event = setup_event(tmp_path)
    client = FakeClient()
    client.error = HpcMcpError("CODEX_REQUEST_FAILED", "private secret", details={
        "rpc_method": "turn/start", "rpc_code": -32000, "data": "private secret"})
    await delivery(store, client).deliver_once()
    first = store.diagnostics(event.id)["attempts"][0]
    assert first["stage"] == "SEND_ACK"
    assert first["outcome"] == "DEFERRED"
    assert (first["rpc_method"], first["rpc_code"]) == ("turn/start", -32000)
    assert b"private secret" not in store.path.read_bytes()
    await delivery(store, FakeClient(), clock=lambda: 1000).deliver_once()
    assert store.events()[0].delivery_state == "MANUAL_REQUIRED"
    assert store.diagnostics(event.id)["attempts"][0]["outcome"] == "MANUAL_REQUIRED"


@pytest.mark.asyncio
@pytest.mark.parametrize("cleanup_fails", [False, True])
async def test_uncertain_send_manual_transition_retains_cleanup(tmp_path, cleanup_fails):
    from hpc_mcp.agent.delivery import DeliveryService
    store, _, event = setup_event(tmp_path)
    first = FakeClient()
    first.error = HpcMcpError("CODEX_REQUEST_FAILED", "synthetic")
    await delivery(store, first).deliver_once()
    client = FakeClient()
    class Manager:
        async def __aenter__(self):
            return client
        async def __aexit__(self, *args):
            if cleanup_fails:
                raise HpcMcpError("CODEX_CHILD_CLEANUP_UNCONFIRMED", "synthetic")
    async def loader():
        return contract()
    service = DeliveryService(store, schema_loader=loader,
        client_factory=lambda schema: Manager(), clock=lambda: 1000)
    await service.deliver_once()
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["outcome"] == "MANUAL_REQUIRED"
    assert row["error_code"] == "CODEX_SEND_OUTCOME_UNKNOWN"
    assert row["cleanup_state"] == ("UNCONFIRMED" if cleanup_fails else "CONFIRMED")
    assert row["cleanup_error_code"] == ("CODEX_CHILD_CLEANUP_UNCONFIRMED" if cleanup_fails else None)
    final = store.events()[0]
    assert final.delivery_state == "MANUAL_REQUIRED"
    assert final.attempt_count == 2
    assert final.send_started_at is not None
    assert all(method not in {"thread/resume", "turn/start"} for method, _ in client.calls)
    assert store.claim_event("another", now=2000) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("after_send", [False, True])
async def test_cancelled_delivery_journal_preserves_send_fence(tmp_path, after_send):
    store, _, event = setup_event(tmp_path)
    client = FakeClient()
    entered = asyncio.Event()
    original = client.request
    async def blocking(method, params):
        if method == ("turn/start" if after_send else "thread/read"):
            entered.set()
            await asyncio.Future()
        return await original(method, params)
    client.request = blocking
    task = asyncio.create_task(delivery(store, client).deliver_once())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["outcome"] == "CANCELLED"
    assert row["error_code"] == ("CODEX_CANCELLED_AFTER_SEND" if after_send else "CODEX_CANCELLED_BEFORE_SEND")
    assert (store.events()[0].send_started_at is not None) == after_send


@pytest.mark.asyncio
async def test_journal_cleanup_does_not_erase_failure_stage(tmp_path):
    store, _, event = setup_event(tmp_path)
    await delivery(store, FakeClient(status="active")).deliver_once()
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["stage"] == "FIRST_HISTORY"
    assert row["error_code"] == "CODEX_THREAD_ACTIVE"
    assert row["cleanup_state"] == "CONFIRMED"
    store2, _, event2 = setup_event(tmp_path / "accepted")
    service = delivery(store2, FakeClient())
    await service.deliver_once()
    await service.wait_idle()
    row = store2.diagnostics(event2.id)["attempts"][0]
    assert row["stage"] == "ACCEPTED"
    assert row["cleanup_state"] == "CONFIRMED"


@pytest.mark.asyncio
async def test_journal_preserves_primary_failure_when_cleanup_also_fails(tmp_path):
    from hpc_mcp.agent.delivery import DeliveryService
    store, _, event = setup_event(tmp_path)
    class Manager:
        async def __aenter__(self):
            return FakeClient(status="active")
        async def __aexit__(self, *args):
            raise OSError("private cleanup secret")
    async def loader():
        return contract()
    service = DeliveryService(store, schema_loader=loader, client_factory=lambda schema: Manager(), clock=lambda: 100)
    await service.deliver_once()
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["stage"] == "FIRST_HISTORY"
    assert row["error_code"] == "CODEX_THREAD_ACTIVE"
    assert row["cleanup_state"] == "UNCONFIRMED"
    assert row["cleanup_error_code"] == "CODEX_CHILD_CLEANUP_UNCONFIRMED"
    assert b"private cleanup secret" not in store.path.read_bytes()


@pytest.mark.asyncio
@pytest.mark.parametrize("code", ["CODEX_UNAVAILABLE", "CODEX_PROTOCOL_INCOMPATIBLE", "CODEX_PRIVATE_SECRET"])
async def test_schema_failure_journal_allowlists_error_codes(tmp_path, code):
    store, _, event = setup_event(tmp_path)
    async def unavailable():
        raise HpcMcpError(code, "private schema secret")
    await delivery(store, FakeClient(), schema_loader=unavailable).deliver_once()
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["stage"] == "SCHEMA"
    assert row["error_code"] == (code if code != "CODEX_PRIVATE_SECRET" else "CODEX_HANDOFF_FAILED")
    assert row["outcome"] == "DEFERRED"
    assert b"private schema secret" not in store.path.read_bytes()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "paginated"])
@pytest.mark.parametrize("with_marker", [False, True])
@pytest.mark.parametrize("content", [[None], [7], [{}], [{"type": 7}],
    [{"type": "text"}], [{"type": "text", "text": 7}],
    [{"type": "text", "text": "ordinary", "text_elements": None}],
    [{"type": "text", "text": "ordinary"}, None]])
async def test_malformed_user_content_defers_without_send_intent(tmp_path, mode, with_marker, content):
    store, watch, event = setup_event(tmp_path)
    items = marker_turn(event, watch)["items"] if with_marker else []
    items.append({"type": "userMessage", "content": content})
    client = FakeClient(history=[{"items": items}])
    client.thread["historyMode"] = mode
    client.pages = [{"data": [{"item": item} for item in items], "nextCursor": None}] * 2
    service = delivery(store, client)
    await service.deliver_once()
    await service.wait_idle()
    result = store.events()[0]
    assert result.delivery_state == "PENDING"
    assert result.last_error == "CODEX_HISTORY_INCOMPLETE"
    assert result.send_started_at is None
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("branch", ["summary", "full"])
@pytest.mark.parametrize("mode", [[], {}])
async def test_unhashable_history_mode_defers_safely(tmp_path, branch, mode):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    original = client.request
    async def malformed(method, params):
        response = await original(method, params)
        if method == "thread/read" and params["includeTurns"] == (branch == "full"):
            response["thread"]["historyMode"] = mode
        return response
    client.request = malformed
    await delivery(store, client).deliver_once()
    result = store.events()[0]
    assert result.last_error == "CODEX_HISTORY_INCOMPLETE"
    assert result.send_started_at is None
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "paginated"])
@pytest.mark.parametrize("content", [[], [{"type": "image", "url": "https://example.invalid/image"}],
    [{"type": "localImage", "path": "attachment.png"}],
    [{"type": "text", "text": "first"}, {"type": "text", "text": "second"}],
    [{"type": "text", "text": "ordinary", "text_elements": [{"text": "attachment"}]}]])
async def test_valid_nonmatching_user_content_allows_delivery(tmp_path, mode, content):
    store, _, _ = setup_event(tmp_path)
    item = {"type": "userMessage", "content": content}
    client = FakeClient(history=[{"items": [item]}])
    client.thread["historyMode"] = mode
    client.pages = [{"data": [{"item": item}], "nextCursor": None}] * 2
    service = delivery(store, client)
    await service.deliver_once()
    await service.wait_idle()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert [m for m, _ in client.calls].count("turn/start") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("history_mode", ["legacy", "paginated"])
@pytest.mark.parametrize("content_kind", ["quote", "wrong-client", "no-client", "extra-content"])
async def test_quote_or_unproven_client_identity_never_proves_delivery(tmp_path, history_mode, content_kind):
    from hpc_mcp.agent.delivery import completion_message
    store, watch, event = setup_event(tmp_path)
    item = {"type": "userMessage", "clientId": event.id, "content": [{
        "type": "text", "text": completion_message(watch, event)}]}
    if content_kind == "quote":
        item["content"][0]["text"] = "Please explain this quoted text: " + item["content"][0]["text"]
    elif content_kind == "wrong-client":
        item["clientId"] = "other-event"
    elif content_kind == "no-client":
        del item["clientId"]
    else:
        item["content"][0]["text_elements"] = [{"text": "other attached input"}]
    client = FakeClient("active", history=[{"items": [item]}] if history_mode == "legacy" else [])
    client.thread["historyMode"] = history_mode
    client.pages = [{"data": [], "nextCursor": "page2"},
                    {"data": [{"item": item}], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state != "DELIVERED"
    assert not any(method == "turn/start" for method, _ in client.calls)


@pytest.mark.asyncio
async def test_no_pending_event_never_starts_schema_or_child(tmp_path):
    from hpc_mcp.agent.delivery import DeliveryService
    def forbidden(*args):
        pytest.fail("no event must mean no Codex process")
    service = DeliveryService(AgentStore(tmp_path / "db"), schema_loader=forbidden,
                              client_factory=forbidden)
    assert not await service.deliver_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("state", sorted(TERMINAL_STATES))
async def test_success_and_failure_payloads_are_small_and_scoped(tmp_path, state):
    store, watch, event = setup_event(tmp_path, state)
    client = FakeClient()
    assert await delivery(store, client).deliver_once()
    assert [c[0] for c in client.calls] == ["thread/read", "thread/read", "thread/turns/list",
        "thread/resume", "thread/read", "thread/read", "thread/turns/list", "turn/start"]
    assert [p["includeTurns"] for m, p in client.calls if m == "thread/read"] == [False, True, False, True]
    payload = client.calls[-1][1]
    assert payload["threadId"] == watch.thread_id
    assert payload["clientUserMessageId"] == event.id
    assert payload["turnTrigger"] == "hpc-slurm-completion"
    message = payload["input"][0]["text"]
    assert f"[hpc-event:{event.id}]" in message
    assert state in message and '"job_id": "123"' in message
    assert "password" not in message and "original-session" not in message and watch.cwd not in message
    assert len(message) < 2000
    assert "do not" in message.lower()
    if state == "COMPLETED":
        assert "AGENTS.md" in message and "sacct" in message and "download" in message
    else:
        assert "diagnostic" in message and "Do not" in message and "resubmit" in message
    result = store.events()[0]
    assert result.delivery_state == "DELIVERED" and result.delivered_at == 100
    assert not await delivery(store, FakeClient()).deliver_once()


@pytest.mark.asyncio
async def test_active_thread_defers_60_seconds_without_resume_or_steer(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient("active")
    await delivery(store, client).deliver_once()
    assert [m for m, _ in client.calls] == ["thread/read"]
    event = store.events()[0]
    assert event.delivery_state == "PENDING" and event.next_retry_at == 160
    assert event.last_error == "CODEX_THREAD_ACTIVE"


@pytest.mark.asyncio
async def test_not_loaded_resumes_and_rechecks_active_after_resume(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient("notLoaded")
    client.on_resume = lambda: client.thread.update(status={"type": "active"})
    await delivery(store, client).deliver_once()
    assert "turn/start" not in [m for m, _ in client.calls]
    assert store.events()[0].last_error == "CODEX_THREAD_ACTIVE"


@pytest.mark.asyncio
async def test_lost_response_restart_finds_marker_without_second_turn(tmp_path):
    store, watch, event = setup_event(tmp_path)
    client = FakeClient()
    client.on_start = lambda params: client.thread["turns"].append(marker_turn(event, watch))
    client.error = HpcMcpError("CODEX_CONNECTION_LOST", "raw child details")
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "PENDING"
    assert store.events()[0].send_started_at == 100
    assert store.events()[0].last_error == "CODEX_CONNECTION_LOST"
    await delivery(AgentStore(store.path), client, clock=lambda: 161).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert [m for m, _ in client.calls].count("turn/start") == 1


@pytest.mark.asyncio
async def test_lost_response_without_marker_requires_manual_resolution_after_backoff(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.error = TimeoutError()
    await delivery(store, client).deliver_once()
    assert not await delivery(store, client, clock=lambda: 159).deliver_once()
    client.error = None
    await delivery(store, client, clock=lambda: 160).deliver_once()
    assert store.events()[0].delivery_state == "MANUAL_REQUIRED"
    assert [m for m, _ in client.calls].count("turn/start") == 1


@pytest.mark.asyncio
async def test_existing_marker_must_be_in_user_message_not_tool_output(tmp_path):
    store, _, event = setup_event(tmp_path)
    client = FakeClient(history=[{"items": [{"type": "commandExecution", "text": f"[hpc-event:{event.id}]"}]}])
    await delivery(store, client).deliver_once()
    assert [m for m, _ in client.calls].count("turn/start") == 1


@pytest.mark.asyncio
async def test_paginated_history_checks_each_page_before_retry(tmp_path):
    store, watch, event = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [], "nextCursor": "next"},
        {"data": [{"turnId": "prior", "item": marker_turn(event, watch)["items"][0]}], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert [m for m, _ in client.calls] == ["thread/read", "thread/items/list", "thread/items/list"]


@pytest.mark.asyncio
async def test_large_paginated_history_uses_summary_and_twenty_item_pages_twice(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient("notLoaded")
    client.thread["historyMode"] = "paginated"
    client.on_resume = lambda: client.thread.update(status={"type": "idle"})
    original = client.request
    async def bounded(method, params):
        if method == "thread/read" and params["includeTurns"]:
            raise HpcMcpError("CODEX_OUTPUT_LIMIT", "large full history")
        if method == "thread/items/list":
            client.calls.append((method, params))
            offset = int(params.get("cursor", "0"))
            end = min(offset + params["limit"], 4567)
            return {"data": [{"turnId": "prior", "item": {"type": "agentMessage",
                "text": "x" * 1100}} for _ in range(offset, end)],
                "nextCursor": str(end) if end < 4567 else None}
        return await original(method, params)
    client.request = bounded
    service = delivery(store, client)
    await service.deliver_once()
    await service.wait_idle()
    assert store.events()[0].delivery_state == "DELIVERED"
    reads = [p for m, p in client.calls if m == "thread/read"]
    assert reads == [{"threadId": "original-thread", "includeTurns": False}] * 2
    pages = [p for m, p in client.calls if m == "thread/items/list"]
    assert len(pages) == 458 and all(p["limit"] == 20 for p in pages)
    assert [p for m, p in client.calls if m == "thread/resume"] == [{
        "threadId": "original-thread", "cwd": str(tmp_path), "excludeTurns": True}]


@pytest.mark.asyncio
async def test_legacy_empty_summary_requires_full_read_to_find_marker(tmp_path):
    store, watch, event = setup_event(tmp_path)
    client = FakeClient(history=[marker_turn(event, watch)])
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert client.calls == [("thread/read", {"threadId": "original-thread", "includeTurns": False}),
                            ("thread/read", {"threadId": "original-thread", "includeTurns": True})]


@pytest.mark.asyncio
@pytest.mark.parametrize("page", [
    {"data": [None], "nextCursor": None},
    {"data": [{"item": {}}], "nextCursor": None},
    {"data": [{"item": {"type": "agentMessage"}}] * 21, "nextCursor": None},
    {"data": [], "nextCursor": ""},
    {"data": [], "nextCursor": 1},
    {"data": []},
])
async def test_invalid_paginated_entries_and_cursors_fail_closed(tmp_path, page):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [page, {"data": [], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_HISTORY_INCOMPLETE"
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
async def test_marker_on_malformed_page_is_not_accepted(tmp_path):
    store, watch, event = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [{"item": marker_turn(event, watch)["items"][0]}], "nextCursor": 1}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "PENDING"
    assert store.events()[0].last_error == "CODEX_HISTORY_INCOMPLETE"


@pytest.mark.asyncio
@pytest.mark.parametrize("capability", ["excludeTurns", "items-method", "cursor"])
async def test_generated_schema_omission_never_sends_unsupported_fields(tmp_path, capability):
    from hpc_mcp.agent.app_server import ProtocolSchema
    from tests.unit.test_app_server import schema_bundle
    raw = schema_bundle()
    if capability == "items-method":
        raw["oneOf"] = [b for b in raw["oneOf"]
                        if b["properties"]["method"]["enum"] != ["thread/items/list"]]
    else:
        definition = "ThreadResumeParams" if capability == "excludeTurns" else "ThreadItemsListParams"
        del raw["definitions"][definition]["properties"][capability]
    schema = ProtocolSchema(raw, contract().notifications)
    async def loader():
        return schema
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [], "nextCursor": "next"}, {"data": [], "nextCursor": None}] * 2
    service = delivery(store, client, schema_loader=loader)
    await service.deliver_once()
    await service.wait_idle()
    if capability == "excludeTurns":
        assert store.events()[0].delivery_state == "DELIVERED"
        assert [p for m, p in client.calls if m == "thread/resume"] == [{
            "threadId": "original-thread", "cwd": str(tmp_path)}]
    else:
        assert store.events()[0].last_error == "CODEX_PROTOCOL_INCOMPATIBLE"
        assert not any(m in {"turn/start", "thread/resume"} for m, _ in client.calls)
        assert not any("cursor" in p for m, p in client.calls if m == "thread/items/list")


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [10000, 10001])
async def test_pagination_item_bound_is_ten_thousand(tmp_path, count):
    from hpc_mcp.agent.delivery import history_has_marker
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [{"item": {"type": "agentMessage"}}] * min(20, count - i),
        "nextCursor": str(i + 20) if i + 20 < count else None} for i in range(0, count, 20)]
    if count == 10000:
        assert not await history_has_marker(client, client.thread, "event", "message")
    else:
        with pytest.raises(HpcMcpError) as error:
            await history_has_marker(client, client.thread, "event", "message")
        assert error.value.code == "CODEX_HISTORY_INCOMPLETE"
    assert len(client.calls) == 500


@pytest.mark.asyncio
async def test_repeated_cursor_fails_closed(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [], "nextCursor": "same"}] * 2
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_HISTORY_INCOMPLETE"
    assert len([m for m, _ in client.calls if m == "thread/items/list"]) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "paginated"])
async def test_uncertain_active_send_reconciles_marker_and_never_replays(tmp_path, mode):
    store, watch, event = setup_event(tmp_path)
    store.claim_event("old", now=1, lease_seconds=1)
    store.mark_send_started(event.id, "old", now=1)
    client = FakeClient("active", history=[marker_turn(event, watch)])
    client.thread["historyMode"] = mode
    client.pages = [{"data": [{"item": marker_turn(event, watch)["items"][0]}], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
async def test_second_scan_finds_delivery_added_during_resume(tmp_path):
    store, watch, event = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [], "nextCursor": None},
        {"data": [{"item": marker_turn(event, watch)["items"][0]}], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert not any(m == "turn/start" for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("context", ["id", "sessionId"])
async def test_resume_wrong_original_context_is_rejected(tmp_path, context):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.on_resume = lambda: client.thread.update({context: "other"})
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_THREAD_CONTEXT_INVALID"
    assert not any(m == "turn/start" for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "paginated", "unknown"])
async def test_active_unsent_thread_skips_history_and_unknown_mode_fails_closed(tmp_path, mode):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient("active")
    client.thread["historyMode"] = mode
    await delivery(store, client).deliver_once()
    assert client.calls == [("thread/read", {"threadId": "original-thread", "includeTurns": False})]
    assert store.events()[0].last_error == (
        "CODEX_HISTORY_INCOMPLETE" if mode == "unknown" else "CODEX_THREAD_ACTIVE")


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["legacy", "paginated"])
async def test_failed_full_history_defers_without_send_intent(tmp_path, mode):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    client.thread["historyMode"] = mode
    original = client.request
    async def limited(method, params):
        if method == "thread/items/list" or (method == "thread/read" and params["includeTurns"]):
            client.calls.append((method, params))
            raise HpcMcpError("CODEX_OUTPUT_LIMIT", "untrusted detail")
        return await original(method, params)
    client.request = limited
    await delivery(store, client).deliver_once()
    result = store.events()[0]
    assert result.last_error == "CODEX_OUTPUT_LIMIT" and result.send_started_at is None
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
async def test_legacy_partial_history_never_proves_marker(tmp_path):
    store, watch, event = setup_event(tmp_path)
    turn = marker_turn(event, watch) | {"itemsView": "partial"}
    client = FakeClient(history=[turn])
    await delivery(store, client).deliver_once()
    assert store.events()[0].last_error == "CODEX_HISTORY_INCOMPLETE"
    assert store.events()[0].delivery_state == "PENDING"


@pytest.mark.asyncio
async def test_uncertain_active_paginated_send_without_marker_requires_manual_resolution(tmp_path):
    store, _, event = setup_event(tmp_path)
    store.claim_event("old", now=1, lease_seconds=1)
    store.mark_send_started(event.id, "old", now=1)
    client = FakeClient("active")
    client.thread["historyMode"] = "paginated"
    client.pages = [{"data": [], "nextCursor": None}]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "MANUAL_REQUIRED"
    assert store.events()[0].handoff_error == "CODEX_SEND_OUTCOME_UNKNOWN"
    assert not any(m in {"thread/resume", "turn/start"} for m, _ in client.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("problem", ["bad-session", "bad-id", "bad-state", "bad-history"])
async def test_invalid_thread_response_defers_without_start(tmp_path, problem):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    if problem == "bad-session": client.thread["sessionId"] = "someone-else"
    if problem == "bad-id": client.thread["id"] = "someone-else"
    if problem == "bad-state": client.thread["status"] = {"type": "systemError"}
    if problem == "bad-history": del client.thread["turns"]
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "PENDING"
    assert "turn/start" not in [m for m, _ in client.calls]


@pytest.mark.asyncio
async def test_schema_error_persisted_and_manual_recovery_is_nonmutating(tmp_path):
    from hpc_mcp.agent.delivery import DeliveryService, manual_recovery
    store, watch, event = setup_event(tmp_path)
    async def bad_schema():
        raise HpcMcpError("CODEX_PROTOCOL_INCOMPATIBLE", "untrusted raw diagnostic")
    await DeliveryService(store, schema_loader=bad_schema, clock=lambda: 100).deliver_once()
    assert store.events()[0].last_error == "CODEX_PROTOCOL_INCOMPATIBLE"
    recovery = manual_recovery(store, event.id)
    assert recovery["thread_id"] == watch.thread_id
    assert event.id in recovery["message"]
    assert "untrusted" not in json.dumps(recovery)
    assert store.events()[0].delivery_state == "PENDING"


def test_send_fencing_and_crash_reclaim_keep_intent(tmp_path):
    store, _, event = setup_event(tmp_path)
    store.claim_event("dead", now=10, lease_seconds=10)
    assert store.mark_send_started(event.id, "dead", now=11)
    assert not store.mark_send_started(event.id, "dead", now=21)
    recovered = AgentStore(store.path).claim_event("recovery", now=22)
    assert recovered.send_started_at == 11 and recovered.attempt_count == 2
    assert not store.mark_send_started(event.id, "dead", now=22)


@pytest.mark.asyncio
async def test_live_delivery_lease_prevents_second_worker(tmp_path):
    store, _, _ = setup_event(tmp_path)
    started, release = asyncio.Event(), asyncio.Event()
    first = FakeClient()
    original = first.request
    async def slow(method, params):
        if method == "turn/start":
            started.set()
            await release.wait()
        return await original(method, params)
    first.request = slow
    task = asyncio.create_task(delivery(store, first).deliver_once())
    await asyncio.wait_for(started.wait(), 3)
    assert not await delivery(AgentStore(store.path), FakeClient()).deliver_once()
    release.set()
    await task
    assert store.events()[0].attempt_count == 1


@pytest.mark.asyncio
async def test_pending_batch_is_bounded_and_oldest_first(tmp_path):
    store, _, first = setup_event(tmp_path)
    second = setup_event(tmp_path, job="124")[2]
    third = setup_event(tmp_path, job="125")[2]
    client = FakeClient()
    service = delivery(store, client)
    await service.deliver_pending(limit=2)
    await service.wait_idle()
    ids = [params["clientUserMessageId"] for method, params in client.calls if method == "turn/start"]
    assert ids == sorted([first.id, second.id, third.id])[:2]
    assert sum(e.delivery_state == "DELIVERED" for e in store.events()) == 2


@pytest.mark.asyncio
async def test_not_loaded_becomes_idle_and_delivers(tmp_path):
    store, _, _ = setup_event(tmp_path)
    client = FakeClient("notLoaded")
    client.on_resume = lambda: client.thread.update(status={"type": "idle"})
    await delivery(store, client).deliver_once()
    assert store.events()[0].delivery_state == "DELIVERED"


@pytest.mark.asyncio
async def test_slow_handoff_session_does_not_block_monitor_cycles(tmp_path):
    from hpc_mcp.agent.monitor import Monitor
    store, _, _ = setup_event(tmp_path)
    client = FakeClient()
    release, started, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()
    async def slow(*_):
        started.set()
        await release.wait()
        return "COMPLETED"
    client.wait_turn_completed = slow
    service = delivery(store, client)
    cycles = []
    async def wait(_):
        await asyncio.sleep(0)
        cycles.append(store.events()[0].handoff_state)
        if len(cycles) == 5:
            stop.set()
    monitor = Monitor(store, None, lambda: "version", wait=wait, after_poll=service.deliver_pending)
    await asyncio.wait_for(monitor.run(stop), 2)
    assert started.is_set() and cycles.count("RUNNING") >= 2
    release.set()
    await service.wait_idle()
    assert store.events()[0].handoff_state == "COMPLETED"


def test_abrupt_agent_restart_persists_unknown_without_reclaiming_accepted_event(tmp_path):
    store, _, event = setup_event(tmp_path)
    store.claim_event("worker", now=10)
    store.mark_delivered(event.id, "worker", now=11, turn_id="accepted", handoff_state="RUNNING")
    restarted = AgentStore(store.path)
    restarted.recover_interrupted_handoffs(now=20)
    result = restarted.events()[0]
    assert result.handoff_state == "UNKNOWN" and result.handoff_error == "CODEX_AGENT_RESTARTED"
    assert result.turn_id == "accepted" and result.delivery_state == "DELIVERED"
    assert restarted.claim_event("other", now=999) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["FAILED", "INTERRUPTED"])
async def test_streamed_failure_is_distinct_from_delivery_receipt(tmp_path, state):
    store, _, event = setup_event(tmp_path)
    client = FakeClient()
    async def outcome(*_):
        return state
    client.wait_turn_completed = outcome
    service = delivery(store, client)
    await service.deliver_once()
    await service.wait_idle()
    assert store.events()[0].handoff_state == state
    assert store.events()[0].delivery_state == "DELIVERED"


@pytest.mark.asyncio
@pytest.mark.parametrize("client_id", [None, "manual-client-id"])
async def test_exact_manual_paste_without_event_client_id_is_frozen_for_human_reconciliation(tmp_path, client_id):
    from hpc_mcp.agent.delivery import completion_message, manual_recovery
    store, watch, event = setup_event(tmp_path)
    client = FakeClient(history=[{"items": [{"type": "userMessage", "clientId": client_id, "content": [{
        "type": "text", "text": completion_message(watch, event)}]}]}])
    await delivery(store, client).deliver_once()
    result = store.events()[0]
    assert result.delivery_state == "MANUAL_REQUIRED"
    assert result.handoff_error == "CODEX_HISTORY_IDENTITY_UNAVAILABLE"
    assert not await delivery(store, client, clock=lambda: 999).deliver_once()
    assert "lacks the event client ID" in manual_recovery(store, event.id)["instructions"]
    assert not any(method == "turn/start" for method, _ in client.calls)
