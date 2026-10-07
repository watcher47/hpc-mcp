import asyncio
import json
import sys
from pathlib import Path

import pytest

from hpc_mcp.agent.app_server import AppServerClient
from hpc_mcp.agent.queued_delivery import QueueDeliveryService
from hpc_mcp.agent.queue_store import QueueStore
from tests.unit.test_agent_delivery import setup_event
from tests.unit.test_queued_delivery import queue_schema


def runtime(store, state, now):
    children = []
    fixture = Path(__file__).parents[1] / 'fixtures' / 'fake_queue_server.py'
    assert fixture.is_file(), 'bounded queue JSONL fixture is missing'
    def factory(schema):
        child = AppServerClient(schema, command=[sys.executable, '-u', str(fixture), str(state)])
        children.append(child)
        return child
    async def loader():
        return queue_schema()
    return QueueDeliveryService(store, schema_loader=loader, client_factory=factory,
        clock=lambda: now[0], observation_interval=.02), children


@pytest.mark.asyncio
async def test_one_large_complete_scan_fits_original_aggregate_cap(tmp_path):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / 'queue.json'
    state.write_text(json.dumps({'history_count': 4567}))
    svc, children = runtime(store, state, [100])
    await svc.deliver_once()
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    calls = json.loads(state.read_text())['calls']
    assert sum(c['method'] == 'thread/items/list' for c in calls) == 229
    assert sum(c['method'] == 'thread/queue/add' for c in calls) == 1
    assert not any(c['method'] in {'thread/resume', 'turn/start', 'turn/steer', 'thread/queue/start'} for c in calls)
    assert 8*1024*1024 < children[0]._output_bytes < 16*1024*1024
    assert children[0].max_line_bytes == 4194304 and children[0].max_output_bytes == 16777216
    assert children[0].process.returncode is not None


@pytest.mark.asyncio
@pytest.mark.parametrize('bad', ['oversized', 'malformed', 'aggregate'])
async def test_bounded_bad_output_never_enqueues_or_exposes_transcript(tmp_path, bad, caplog):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / 'queue.json'
    state.write_text(json.dumps({'bad': bad, 'history_count': 9000 if bad == 'aggregate' else 0}))
    svc, children = runtime(store, state, [100])
    await svc.deliver_once()
    assert QueueStore(store).get(event.id) is None
    assert store.events()[0].send_started_at is None
    assert store.events()[0].last_error == ('CODEX_PROTOCOL_INVALID' if bad == 'malformed' else 'CODEX_OUTPUT_LIMIT')
    assert children[0].process.returncode is not None
    assert 'private transcript' not in caplog.text


@pytest.mark.asyncio
async def test_cold_resume_jsonl_owned_until_target_completes(tmp_path):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / 'queue.json'
    state.write_text(json.dumps({'status': 'notLoaded', 'delay': .1}))
    now = [100]
    svc, children = runtime(store, state, now)
    await svc.deliver_once()
    assert children[0].process.returncode is not None
    now[0] = 151
    await svc.observe_once()
    assert svc._owned_sessions
    assert children[1].process.returncode is None
    await asyncio.sleep(.2)
    now[0] = 202
    await asyncio.wait_for(svc.wait_idle(), 3)
    receipt = QueueStore(store).get(event.id)
    assert receipt.state == 'COMPLETED' and receipt.turn_id == 'queued-turn'
    assert all(c.process.returncode is not None for c in children)
    calls = json.loads(state.read_text())['calls']
    assert sum(c['method'] == 'thread/queue/add' for c in calls) == 1
    assert [c['params'] for c in calls if c['method'] == 'thread/resume'] == [
        {'threadId': 'original-thread', 'excludeTurns': True}]
    assert not any(c['method'] in {'turn/start', 'turn/steer', 'thread/queue/start'} for c in calls)


@pytest.mark.asyncio
async def test_desktop_null_client_jsonl_reconciles_ack_conflict_without_replay(tmp_path):
    store, _, event = setup_event(tmp_path)
    state = tmp_path / 'queue.json'
    state.write_text(json.dumps({'history_count': 21}))
    now = [100]
    svc, children = runtime(store, state, now)
    await svc.deliver_once()
    queue = QueueStore(store)
    claim = queue.claim_observation('previous-observer', now=101)
    queue.record_observation(claim, 'previous-observer', now=102, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    data = json.loads(state.read_text())
    entry = data['queue'].pop()
    data['turns'] = [{'id': 'desktop-turn', 'status': 'interrupted', 'completedAt': None,
        'durationMs': None, 'items': [{'id': 'desktop-input', 'type': 'userMessage',
        'clientId': None, 'content': entry['input']}]}]
    state.write_text(json.dumps(data))
    now[0] = 153
    assert await svc.observe_once()
    assert queue.get(event.id).state == 'OBSERVING'
    assert queue.get(event.id).identity_kind == 'ACK_MESSAGE_NATIVE_ITEM'
    assert store.events()[0].handoff_state == 'RUNNING'
    data = json.loads(state.read_text())
    data['turns'][0].update(status='completed', completedAt=190, durationMs=90000)
    state.write_text(json.dumps(data))
    now[0] = 204
    assert await svc.observe_once()
    assert queue.get(event.id).state == 'COMPLETED'
    assert queue.get(event.id).item_id == 'desktop-input'
    calls = json.loads(state.read_text())['calls']
    assert sum(c['method'] == 'thread/queue/add' for c in calls) == 1
    assert sum(c['method'] == 'thread/items/list' for c in calls) == 6
    assert not any(c['method'] in {'thread/resume', 'turn/start', 'turn/steer', 'thread/queue/start'} for c in calls)
    assert all(c.process.returncode is not None for c in children)
