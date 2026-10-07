import json
import asyncio
import sys
import sqlite3
from pathlib import Path

import pytest

from hpc_mcp.agent.app_server import AppServerClient
from hpc_mcp.agent.delivery import DeliveryService
from hpc_mcp.agent.store import AgentStore
from tests.unit.test_agent_delivery import setup_event
from tests.unit.test_app_server import contract


@pytest.mark.asyncio
@pytest.mark.parametrize("lose_response", [False, True])
async def test_large_paginated_jsonl_history_stays_within_original_limits(tmp_path, lose_response):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / "large-history.json"
    state.write_text(json.dumps({"history_mode": "paginated", "history_item_count": 4567,
        "lose_response": lose_response, "status": "notLoaded"}))
    children = []
    def factory(schema):
        child = AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child
    async def schema():
        return contract()
    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    await service.deliver_once()
    await service.wait_idle()
    if lose_response:
        assert store.events()[0].send_started_at == 100
        assert store.events()[0].delivery_state == "PENDING"
        restarted = DeliveryService(AgentStore(store.path), schema_loader=schema,
            client_factory=factory, clock=lambda: 161)
        await restarted.deliver_once()
        await restarted.wait_idle()
    assert store.events()[0].delivery_state == "DELIVERED"
    calls = json.loads(state.read_text())["calls"]
    assert sum(c["method"] == "turn/start" for c in calls) == 1
    assert all(not c["params"]["includeTurns"] for c in calls if c["method"] == "thread/read")
    assert all(c["params"]["excludeTurns"] for c in calls if c["method"] == "thread/resume")
    pages = [c for c in calls if c["method"] == "thread/items/list"]
    assert len(pages) == 229 * (3 if lose_response else 2)
    assert all(c["params"]["limit"] == 20 for c in pages)
    latest = [c for c in calls if c["method"] == "thread/turns/list"]
    assert len(latest) == 2
    assert all(c["params"] == {"threadId": "original-thread", "limit": 1,
        "sortDirection": "desc", "itemsView": "notLoaded"} for c in latest)
    assert all(child.process.returncode is not None for child in children)
    assert all(child.max_line_bytes == 4194304 and child.max_output_bytes == 16777216 for child in children)
    assert children[0]._output_bytes > 2 * 4194304
    assert json.loads(state.read_text())["turns"][0]["items"][0]["clientId"] == event.id


@pytest.mark.asyncio
async def test_jsonl_unloaded_runtime_with_interrupted_persisted_turn_defers(tmp_path):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / "readiness.json"
    state.write_text(json.dumps({"status": "notLoaded", "latest_status": "interrupted"}))
    def factory(schema):
        return AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
    async def schema():
        return contract()
    await DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100).deliver_once()
    assert store.events()[0].last_error == "CODEX_THREAD_NOT_READY"
    assert store.events()[0].send_started_at is None
    assert store.diagnostics(event.id)["attempts"][0]["stage"] == "READINESS"
    calls = json.loads(state.read_text())["calls"]
    assert not any(c["method"] in {"thread/resume", "turn/start"} for c in calls)


@pytest.mark.asyncio
async def test_shutdown_during_supervisor_cleanup_still_reaps_child(tmp_path):
    store, _, _ = setup_event(tmp_path)
    state = tmp_path / "cleanup-race.json"
    state.write_text(json.dumps({"work_seconds": .1}))
    closing, release = asyncio.Event(), asyncio.Event()
    children = []

    class SlowCleanupClient(AppServerClient):
        async def __aexit__(self, *args):
            closing.set()
            await release.wait()
            await super().__aexit__(*args)

    def factory(schema):
        child = SlowCleanupClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child

    async def schema():
        return contract()

    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    try:
        await service.deliver_once()
        await asyncio.wait_for(closing.wait(), 3)
        shutdown = asyncio.create_task(service.aclose())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        release.set()
        await shutdown
        assert children[0].process.returncode is not None
        assert not service._owned_sessions
    finally:
        release.set()
        for child in children:
            await AppServerClient.__aexit__(child, None, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", [False, True])
@pytest.mark.parametrize("failure", ["persistence", "cleanup", "cleanup-after"])
async def test_outcome_failure_reaps_all_children_without_resending(
    tmp_path, monkeypatch, caplog, shutdown, failure
):
    store, _, _ = setup_event(tmp_path)
    setup_event(tmp_path, job="124")
    children, states, failed_events = [], [], []
    original_finish = store.finish_handoff

    def fail_first(event_id, *args, **kwargs):
        if not failed_events or event_id == failed_events[0]:
            failed_events.append(event_id)
            raise sqlite3.OperationalError("untrusted private database detail")
        return original_finish(event_id, *args, **kwargs)

    if failure == "persistence":
        monkeypatch.setattr(store, "finish_handoff", fail_first)

    class CleanupFailureClient(AppServerClient):
        async def __aexit__(self, *args):
            if self is children[0] and failure == "cleanup":
                raise OSError("untrusted private child detail")
            await super().__aexit__(*args)
            if self is children[0]:
                raise OSError("untrusted private child detail")

    def factory(schema):
        state = tmp_path / f"child-{len(children)}.json"
        state.write_text(json.dumps({"work_seconds": 30 if shutdown else .1}))
        states.append(state)
        cls = CleanupFailureClient if failure.startswith("cleanup") else AppServerClient
        child = cls(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child

    async def schema():
        return contract()

    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    try:
        await service.deliver_once()
        await service.deliver_once()
        if shutdown:
            # The second supervisor has not started yet; aclose's retained
            # ownership fallback must clean it even if the first session fails.
            await service.aclose()
        else:
            await service.wait_idle()
        assert all(child.process.returncode is not None for child in children)
        assert not service._owned_sessions
        assert not service._sessions
        assert "manual-recovery" in caplog.text
        assert "untrusted private" not in caplog.text
        assert all(event.delivery_state == "DELIVERED" for event in store.events())
        assert not await service.deliver_once()
        restarted = AgentStore(store.path)
        restarted.recover_interrupted_handoffs(now=101)
        assert not await DeliveryService(restarted, schema_loader=schema,
            client_factory=factory, clock=lambda: 1000).deliver_once()
        assert len(children) == 2
        assert all(sum(call["method"] == "turn/start" for call in
            json.loads(state.read_text())["calls"]) == 1 for state in states)
    finally:
        # A red test must not leave its intentionally exposed children running.
        for child in children:
            await AppServerClient.__aexit__(child, None, None, None)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["error", "timeout"])
async def test_failed_force_reap_retains_ownership_and_receipt(tmp_path, caplog, failure):
    store, _, _ = setup_event(tmp_path)
    setup_event(tmp_path, job="124")
    children = []
    allow_reap = False

    class BrokenClient(AppServerClient):
        async def __aexit__(self, *args):
            if self is children[0]:
                raise OSError("untrusted private detail")
            await super().__aexit__(*args)

        async def force_reap(self):
            if self is children[0] and not allow_reap:
                if failure == "timeout":
                    await asyncio.Event().wait()
                raise OSError("untrusted private detail")
            await super().force_reap()

    def factory(schema):
        state = tmp_path / f"reap-{len(children)}.json"
        state.write_text(json.dumps({"work_seconds": .05}))
        child = BrokenClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child

    async def schema():
        return contract()

    service = DeliveryService(store, schema_loader=schema, client_factory=factory,
                              clock=lambda: 100, cleanup_timeout=.2)
    try:
        await service.deliver_once()
        await service.deliver_once()
        await asyncio.wait_for(service.wait_idle(), 5)
        assert children[0].process.returncode is None
        assert children[1].process.returncode is not None
        assert len(service._owned_sessions) == 1
        assert next(iter(service._owned_sessions.values()))[0] is children[0]
        assert all(event.delivery_state == "DELIVERED" for event in store.events())
        assert any(event.handoff_error == "CODEX_CHILD_CLEANUP_UNCONFIRMED" for event in store.events())
        assert "CODEX_CHILD_CLEANUP_UNCONFIRMED" in caplog.text
        assert "untrusted private" not in caplog.text
        assert not await service.deliver_once()
        restarted = AgentStore(store.path)
        restarted.recover_interrupted_handoffs(now=101)
        assert restarted.claim_event("restarted", now=1000) is None
        allow_reap = True
        await service.aclose()
        assert not service._owned_sessions
        assert all(child.process.returncode is not None for child in children)
    finally:
        for child in children:
            await AppServerClient.__aexit__(child, None, None, None)


@pytest.mark.asyncio
async def test_cancelled_shutdown_finishes_all_owned_cleanup(tmp_path):
    store, _, _ = setup_event(tmp_path)
    setup_event(tmp_path, job="124")
    children = []
    closing, release = asyncio.Event(), asyncio.Event()

    class SlowClient(AppServerClient):
        async def __aexit__(self, *args):
            if self is children[0]:
                closing.set()
                await release.wait()
                raise OSError("failure before base cleanup")
            await super().__aexit__(*args)

    def factory(schema):
        state = tmp_path / f"cancel-{len(children)}.json"
        state.write_text(json.dumps({"work_seconds": 30}))
        child = SlowClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child

    async def schema():
        return contract()

    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    try:
        await service.deliver_once()
        await service.deliver_once()
        shutdown = asyncio.create_task(service.aclose())
        await asyncio.wait_for(closing.wait(), 3)
        shutdown.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(shutdown, 8)
        assert all(child.process.returncode is not None for child in children)
        assert not service._owned_sessions
        assert all(event.delivery_state == "DELIVERED" for event in store.events())
    finally:
        release.set()
        for child in children:
            await AppServerClient.__aexit__(child, None, None, None)


@pytest.mark.asyncio
async def test_acknowledged_long_turn_survives_and_does_not_block_polling(tmp_path):
    store, _, _ = setup_event(tmp_path)
    state = tmp_path / "long-state.json"
    state.write_text(json.dumps({"work_seconds": 1.5}))
    def factory(schema):
        return AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
    async def schema():
        return contract()
    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    await service.deliver_once()
    ticks = 0
    for _ in range(25):
        ticks += 1
        await asyncio.sleep(.1)
        if json.loads(state.read_text()).get("artifact"):
            break
    assert json.loads(state.read_text()).get("artifact") == "analysis-finished"
    assert ticks > 1
    await service.wait_idle()
    assert store.events()[0].handoff_state == "COMPLETED"


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_accepted_turn_loss_requires_manual_recovery_and_never_resubmits(tmp_path, restart):
    store, _, _ = setup_event(tmp_path)
    state = tmp_path / "lost-child.json"
    state.write_text(json.dumps({"work_seconds": 5 if restart else .1, "crash_after_ack": True}))
    children = []
    def factory(schema):
        child = AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(child)
        return child
    async def schema():
        return contract()
    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    await service.deliver_once()
    if restart:
        await service.aclose()
        AgentStore(store.path).recover_interrupted_handoffs(now=101)
    else:
        await service.wait_idle()
    assert store.events()[0].delivery_state == "DELIVERED"
    assert store.events()[0].handoff_state == "UNKNOWN"
    assert store.events()[0].handoff_error
    assert all(child.process.returncode is not None for child in children)
    assert not await DeliveryService(store, schema_loader=schema, client_factory=factory).deliver_once()
    assert sum(c["method"] == "turn/start" for c in json.loads(state.read_text())["calls"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["idle", "notLoaded", "active"])
async def test_real_jsonl_child_handshake_original_thread_and_clean_shutdown(tmp_path, status):
    store, watch, event = setup_event(tmp_path)
    state = tmp_path / "fake-state.json"
    state.write_text(json.dumps({"status": status}))
    children = []
    def factory(schema):
        client = AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
        children.append(client)
        return client
    async def schema():
        return contract()
    service = DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100)
    await service.deliver_once()
    await service.wait_idle()
    result = json.loads(state.read_text())
    assert [req["method"] for req in result["calls"]][:2] == ["initialize", "initialized"]
    assert result["clean_exits"] == 1
    assert all(child.process.returncode == 0 for child in children)
    if status == "active":
        assert store.events()[0].next_retry_at == 160
        assert not result.get("turns")
    else:
        assert store.events()[0].delivery_state == "DELIVERED"
        assert len(result["turns"]) == 1
        assert event.id == result["turns"][0]["items"][0]["clientId"]


@pytest.mark.asyncio
async def test_child_disconnect_restart_has_exactly_one_persisted_marker(tmp_path):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / "fake-state.json"
    state.write_text(json.dumps({"lose_response": True}))
    def factory(schema):
        return AppServerClient(schema, command=[sys.executable, "-u",
            str(Path(__file__).parents[1] / "fixtures" / "fake_app_server.py"), str(state)])
    async def schema():
        return contract()
    await DeliveryService(store, schema_loader=schema, client_factory=factory, clock=lambda: 100).deliver_once()
    assert store.events()[0].delivery_state == "PENDING"
    restarted = AgentStore(store.path)
    await DeliveryService(restarted, schema_loader=schema, client_factory=factory, clock=lambda: 161).deliver_once()
    assert restarted.events()[0].delivery_state == "DELIVERED"
    result = json.loads(state.read_text())
    assert len(result["turns"]) == 1
    assert [req["method"] for req in result["calls"]].count("turn/start") == 1
    assert result["turns"][0]["items"][0]["content"][0]["text"].count(f"[hpc-event:{event.id}]") == 1
