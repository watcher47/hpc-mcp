import asyncio
import copy
from contextlib import asynccontextmanager

import pytest

from hpc_mcp.agent import delivery as legacy
from hpc_mcp.agent.app_server import ProtocolSchema
from hpc_mcp.agent.queue_protocol import render_queue_message
from hpc_mcp.agent.queue_store import QueueStore
from hpc_mcp.errors import HpcMcpError
from tests.unit.test_agent_delivery import setup_event
from tests.unit.test_app_server import schema_bundle


def queue_schema():
    bundle = schema_bundle()
    bundle['definitions']['ThreadTurnsListParams']['properties']['cursor'] = {'type': 'string'}
    for method, fields in [('thread/queue/add', ['threadId', 'clientUserMessageId', 'input']),
                          ('thread/queue/list', ['threadId', 'cursor', 'limit'])]:
        bundle['oneOf'].append({'type': 'object', 'properties': {'id': {'type': 'integer'},
            'method': {'enum': [method]}, 'params': {'type': 'object',
            'properties': {f: {} for f in fields}}}})
    return ProtocolSchema(bundle, {'type': 'object'}, route='queue')


class Native:
    def __init__(self, status='active'):
        self.status, self.queue, self.turns, self.calls = status, [], [], []
        self.error = None
        self.on_add = None
        self.on_resume = None
        self.on_queue = None
        self.closed = 0
        self.opened = 0
        self.pages = None

    async def request(self, method, params):
        self.calls.append((method, copy.deepcopy(params)))
        if method == 'thread/read':
            return {'thread': {'id': 'original-thread', 'sessionId': 'original-session',
                'status': {'type': self.status}, 'historyMode': 'paginated' if self.pages else 'legacy',
                'turns': copy.deepcopy(self.turns) if params['includeTurns'] else []}}
        if method == 'thread/queue/list':
            if self.on_queue:
                self.on_queue()
            return {'data': copy.deepcopy(self.queue), 'nextCursor': None}
        if method == 'thread/items/list':
            return self.pages.pop(0)
        if method == 'thread/turns/list':
            return {'data': [dict(t, items=[], itemsView='notLoaded') for t in self.turns], 'nextCursor': None}
        if method == 'thread/queue/add':
            entry = {'id': 'queued-id', 'clientUserMessageId': params['clientUserMessageId'], 'input': params['input']}
            self.queue.append(entry)
            if self.on_add:
                self.on_add()
            if self.error:
                raise self.error
            return {'queuedSubmission': entry}
        if method == 'thread/resume':
            if self.on_resume:
                self.on_resume()
            return {'thread': {'id': 'original-thread', 'sessionId': 'original-session',
                'status': {'type': 'idle'}, 'turns': [], 'historyMode': 'paginated'}}
        raise AssertionError(method)

    async def drain_notifications(self):
        await asyncio.Event().wait()

    def consume(self, status='completed', completed_at=110):
        q = self.queue.pop(0)
        self.turns.append({'id': 'target', 'status': status, 'completedAt': completed_at,
            'items': [{'id': 'input', 'type': 'userMessage', 'clientId': q['clientUserMessageId'], 'content': q['input']}]})


def service(store, native, now=None, **kwargs):
    import importlib.util
    assert importlib.util.find_spec('hpc_mcp.agent.queued_delivery'), 'queue runtime is missing'
    from hpc_mcp.agent import queued_delivery
    @asynccontextmanager
    async def factory(schema):
        native.opened += 1
        try:
            yield native
        finally:
            native.closed += 1
    async def loader():
        return queue_schema()
    return queued_delivery.QueueDeliveryService(store, schema_loader=loader, client_factory=factory,
        clock=(lambda: now[0]) if now else lambda: 100, **kwargs)


def no_direct(native):
    assert not {'turn/start', 'turn/steer', 'thread/queue/start'} & {m for m, _ in native.calls}


@pytest.mark.asyncio
@pytest.mark.parametrize('missing', [False, True])
async def test_desktop_ack_binds_native_input_and_waits_for_final_accounting(tmp_path, missing):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    native.consume('interrupted', None)
    item = native.turns[0]['items'][0]
    item['clientId'] = None
    if missing:
        del item['clientId']
    journal = store.diagnostics(event.id)['attempts']
    for at, expected in [(151, 'OBSERVING'), (202, 'COMPLETED')]:
        if at == 202:
            native.turns[0].update(status='completed', completedAt=190)
        native.pages = [{'data': [{'item': copy.deepcopy(item), 'turnId': 'target'}], 'nextCursor': None}]
        now[0] = at
        await service(type(store)(store.path), native, now).observe_once()
        receipt = QueueStore(store).get(event.id)
        assert receipt.state == expected
        assert receipt.item_id == 'input' and receipt.turn_id == 'target'
        assert receipt.identity_kind == 'ACK_MESSAGE_NATIVE_ITEM'
        assert store.events()[0].handoff_state == ('RUNNING' if at == 151 else 'COMPLETED')
    assert store.diagnostics(event.id)['attempts'] == journal
    assert sum(m == 'thread/queue/add' for m, _ in native.calls) == 1
    no_direct(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['item', 'turn', 'digest'])
async def test_desktop_bound_identity_cannot_switch(tmp_path, change):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    native.consume('interrupted', None)
    native.turns[0]['items'][0]['clientId'] = None
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'OBSERVING'
    if change == 'item':
        native.turns[0]['items'][0]['id'] = 'replacement'
    elif change == 'turn':
        native.turns[0]['id'] = 'replacement'
    else:
        with store._connect() as db:
            db.execute("UPDATE queue_handoffs SET message_sha256=?", ('0'*64,))
    now[0] = 202
    await service(store, native, now).observe_once()
    receipt = QueueStore(store).get(event.id)
    assert receipt.state == 'CONFLICT'
    assert receipt.item_id == 'input' and receipt.turn_id == 'target'
    assert store.events()[0].handoff_finished_at is None


@pytest.mark.asyncio
@pytest.mark.parametrize('recovery', ['none', 'identity', 'transient'])
@pytest.mark.parametrize('bad', ['no-ack', 'no-queue-id', 'wrong-client', 'duplicate',
    'remaining', 'replacement', 'changed-queue', 'quoted', 'multipart', 'malformed',
    'message', 'context', 'version', 'unknown-version', 'unknown-format', 'incomplete'])
async def test_desktop_fallback_requires_all_ack_and_exact_identity_guards(tmp_path, bad, recovery):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    if recovery != 'none':
        queue = QueueStore(store)
        claim = queue.claim_observation('old-observer', now=101)
        assert queue.record_observation(claim, 'old-observer', now=102, state='CONFLICT',
            error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
        if recovery == 'transient':
            claim = queue.claim_observation('observer', now=153)
            assert queue.record_observation(claim, 'observer', now=154, error_code='CODEX_TIMEOUT')
    queued = copy.deepcopy(native.queue)
    native.consume()
    item = native.turns[0]['items'][0]
    item['clientId'] = None
    if bad in {'no-ack', 'no-queue-id', 'message', 'context', 'version', 'unknown-version', 'unknown-format'}:
        field, value = {'no-ack': ('acknowledged_at', None), 'no-queue-id': ('queue_id', None),
            'message': ('message_sha256', '0'*64), 'context': ('context_sha256', '0'*64),
            'version': ('message_version', 1), 'unknown-version': ('message_version', 99),
            'unknown-format': ('format_version', 99)}[bad]
        with store._connect() as db:
            db.execute(f'UPDATE queue_handoffs SET {field}=?', (value,))
    elif bad == 'wrong-client':
        item['clientId'] = 'wrong'
    elif bad == 'duplicate':
        native.turns[0]['items'].append(dict(item, id='duplicate'))
    elif bad in {'remaining', 'replacement', 'changed-queue'}:
        native.queue = queued
        if bad == 'replacement':
            native.queue[0]['id'] = 'replacement'
        elif bad == 'changed-queue':
            native.queue[0].update(clientUserMessageId='unrelated', input=[{'type': 'text', 'text': 'changed'}])
    elif bad == 'quoted':
        item['content'][0]['text'] = '> ' + item['content'][0]['text']
    elif bad == 'multipart':
        item['content'].append({'type': 'text', 'text': ''})
    elif bad == 'incomplete':
        native.pages = [{'data': [{'item': item, 'turnId': 'target'}], 'nextCursor': 'next'},
            {'data': [], 'nextCursor': 'next'}]
    else:
        item['content'][0]['text_elements'] = 'malformed'
    now[0] = 205 if recovery == 'transient' else 153
    await service(store, native, now).observe_once()
    assert store.events()[0].delivered_at is None
    assert store.events()[0].handoff_finished_at is None
    if bad == 'wrong-client':
        assert QueueStore(store).get(event.id).last_error == 'CODEX_QUEUE_CONFLICT'
    assert sum(m == 'thread/queue/add' for m, _ in native.calls) == 1
    no_direct(native)
    assert not any(m == 'thread/resume' for m, _ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('legacy_send', [False, True])
async def test_desktop_unrecorded_or_legacy_null_client_never_authorizes_send(tmp_path, legacy_send):
    store, watch, event = setup_event(tmp_path)
    if legacy_send:
        store.claim_event('old', now=2)
        store.mark_send_started(event.id, 'old', now=3)
        store.defer_event(event.id, 'old', now=4, retry_at=5, error_code='CODEX_CONNECTION_LOST')
    message = legacy.completion_message(watch, event) if legacy_send else render_queue_message(watch, event)
    native = Native()
    native.turns = [{'id': 'external', 'items': [{'id': 'external-input', 'type': 'userMessage',
        'clientId': None, 'content': [{'type': 'text', 'text': message}]}]}]
    await service(store, native).deliver_once()
    assert QueueStore(store).get(event.id) is None
    assert store.events()[0].delivered_at is None
    assert not any(m in {'thread/queue/add', 'thread/resume'} for m, _ in native.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize('proof', ['valid', 'absent', 'wrong-client', 'queued'])
async def test_existing_desktop_identity_conflict_reobserves_without_send_or_resume(tmp_path, proof):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    await service(store, native, now).deliver_once()
    queue = QueueStore(store)
    claim = queue.claim_observation('old-observer', now=101)
    assert queue.record_observation(claim, 'old-observer', now=102, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    before = store.events()[0]
    journal = store.diagnostics(event.id)['attempts']
    if proof != 'queued':
        native.consume('interrupted', None)
        native.turns[0]['items'][0]['clientId'] = 'wrong' if proof == 'wrong-client' else None
        if proof == 'absent':
            native.turns.clear()
    now[0] = 153
    assert await service(type(store)(store.path), native, now).observe_once()
    receipt = queue.get(event.id)
    assert receipt.state == ('OBSERVING' if proof == 'valid' else 'CONFLICT')
    if proof == 'valid':
        assert receipt.prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
        assert store.events()[0].handoff_state == 'RUNNING'
    else:
        assert store.events()[0].delivered_at is None
    assert store.events()[0].send_started_at == before.send_started_at
    assert store.diagnostics(event.id)['attempts'] == journal
    assert sum(m == 'thread/queue/add' for m, _ in native.calls) == 1
    assert not any(m == 'thread/resume' for m, _ in native.calls)
    assert store.claim_event('replay', now=1000) is None


@pytest.mark.asyncio
async def test_conflict_native_read_timeout_then_valid_proof_without_replay(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    await service(store, native, now).deliver_once()
    queue = QueueStore(store)
    claim = queue.claim_observation('old-observer', now=101)
    assert queue.record_observation(claim, 'old-observer', now=102, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    journal = store.diagnostics(event.id)['attempts']
    def timeout():
        raise HpcMcpError('CODEX_TIMEOUT', 'temporary native read failure')
    native.on_queue = timeout
    now[0] = 153
    assert await service(type(store)(store.path), native, now).observe_once()
    assert queue.get(event.id).state == 'CONFLICT'
    assert queue.get(event.id).last_error == 'CODEX_TIMEOUT'
    assert store.events()[0].delivered_at is None
    native.on_queue = None
    native.consume()
    native.turns[0]['items'][0]['clientId'] = None
    now[0] = 204
    assert await service(type(store)(store.path), native, now).observe_once()
    receipt = queue.get(event.id)
    assert receipt.state == 'COMPLETED' and receipt.item_id == 'input'
    assert receipt.identity_kind == 'ACK_MESSAGE_NATIVE_ITEM'
    assert receipt.prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
    assert receipt.last_error is None
    assert store.events()[0].handoff_state == 'COMPLETED'
    assert store.diagnostics(event.id)['attempts'] == journal
    assert sum(m == 'thread/queue/add' for m, _ in native.calls) == 1
    assert not any(m == 'thread/resume' for m, _ in native.calls)
    no_direct(native)
    assert store.claim_event('replay', now=1000) is None


@pytest.mark.asyncio
async def test_ack_is_queued_and_not_analysis_delivery(tmp_path):
    store, _, event = setup_event(tmp_path)
    native = Native()
    svc = service(store, native)
    await svc.deliver_pending(limit=1)
    await svc.wait_idle()
    result = store.events()[0]
    assert result.delivery_state == 'QUEUED' and result.delivered_at is None
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    assert [m for m, _ in native.calls].count('thread/queue/add') == 1
    assert native.closed == 1
    assert store.diagnostics(event.id)['attempts'][0]['cleanup_state'] == 'CONFIRMED'
    no_direct(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('proof', ['queue', 'history', 'neither'])
async def test_lost_ack_restart_reconciles_without_replay(tmp_path, proof):
    store, _, event = setup_event(tmp_path)
    now, native = [100], Native()
    native.error = HpcMcpError('CODEX_CONNECTION_LOST', 'private secret')
    if proof == 'history':
        native.on_add = lambda: native.consume()
    elif proof == 'neither':
        native.on_add = native.queue.clear
    await service(store, native, now).deliver_once()
    assert store.events()[0].last_error == 'CODEX_CONNECTION_LOST'
    journal = store.diagnostics(event.id)['attempts']
    now[0] = 221
    svc = service(type(store)(store.path), native, now)
    await svc.observe_once()
    receipt = QueueStore(store).get(event.id)
    assert receipt.state == {'queue': 'QUEUED', 'history': 'COMPLETED', 'neither': 'INTENT'}[proof]
    assert store.events()[0].last_error == (None if proof == 'history' else 'CODEX_CONNECTION_LOST')
    assert store.diagnostics(event.id)['attempts'] == journal
    assert journal[0]['error_code'] == 'CODEX_CONNECTION_LOST'
    assert [m for m, _ in native.calls].count('thread/queue/add') == 1
    no_direct(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('state,end,expected', [('completed', 110, 'COMPLETED'),
    ('failed', 110, 'FAILED'), ('interrupted', 110, 'INTERRUPTED'), ('interrupted', None, 'OBSERVING')])
async def test_exact_target_outcome_not_unrelated_latest(tmp_path, state, end, expected):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    native.consume(state, end)
    native.turns.append({'id': 'latest-unrelated', 'status': 'completed', 'completedAt': 200, 'items': []})
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == expected
    assert store.events()[0].turn_id == 'target'
    assert store.events()[0].handoff_state == ('RUNNING' if expected == 'OBSERVING' else expected)
    no_direct(native)


@pytest.mark.asyncio
async def test_queue_disappearance_and_digest_change_are_unresolved(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    native.queue.clear()
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    assert store.events()[0].delivered_at is None
    with store._connect() as db:
        db.execute("UPDATE queue_handoffs SET message_sha256=? WHERE event_id=?", ('0'*64, event.id))
    now[0] = 202
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'CONFLICT'
    assert [m for m, _ in native.calls].count('thread/queue/add') == 1


@pytest.mark.asyncio
async def test_cold_resume_retains_owned_child_until_exact_terminal_evidence(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    svc = service(store, native, now, observation_interval=.01)
    await svc.deliver_once()
    native.on_resume = lambda: native.consume('interrupted', None)
    now[0] = 151
    await svc.observe_once()
    await asyncio.sleep(.025)
    assert svc._owned_sessions and native.closed < native.opened
    assert QueueStore(store).get(event.id).state == 'OBSERVING'
    native.turns[0].update(status='completed', completedAt=170)
    now[0] = 202
    await asyncio.wait_for(svc.wait_idle(), 1)
    assert QueueStore(store).get(event.id).state == 'COMPLETED'
    assert native.closed == native.opened
    assert [p for m, p in native.calls if m == 'thread/resume'] == [
        {'threadId': 'original-thread', 'excludeTurns': True}]
    no_direct(native)


@pytest.mark.asyncio
@pytest.mark.parametrize('status,end,outcome', [
    ('completed', 10, 'COMPLETED'), ('failed', 10, 'FAILED'),
    ('interrupted', 10, 'INTERRUPTED'), ('inProgress', None, 'UNKNOWN'),
    ('interrupted', None, 'UNKNOWN')])
async def test_legacy_uncertain_v1_reconciliation_is_read_only(tmp_path, status, end, outcome):
    store, watch, event = setup_event(tmp_path)
    old = store.claim_event('old', now=2)
    store.mark_send_started(event.id, 'old', now=3)
    store.defer_event(event.id, 'old', now=4, retry_at=5, error_code='CODEX_CONNECTION_LOST')
    prior_attempt = store.diagnostics(event.id)['attempts'][0]
    message = legacy.completion_message(watch, old)
    native = Native()
    native.turns = [{'id': 'legacy', 'status': status, 'completedAt': end, 'items': [
        {'id': 'old-input', 'type': 'userMessage', 'clientId': event.id,
         'content': [{'type': 'text', 'text': message}]}]}]
    await service(store, native).deliver_once()
    result = store.events()[0]
    assert result.delivery_state == 'DELIVERED'
    assert result.handoff_state == outcome
    assert result.handoff_finished_at == (None if outcome == 'UNKNOWN' else 100)
    assert result.last_error is None
    assert result.send_started_at == 3
    assert legacy.completion_message(watch, result) == message
    assert store.diagnostics(event.id)['attempts'][1] == prior_attempt
    assert QueueStore(store).get(event.id) is None
    assert not any(m in {'thread/resume', 'thread/queue/add'} for m, _ in native.calls)
    no_direct(native)
    before = store.events()
    assert not await service(type(store)(store.path), native).deliver_once()
    assert store.events() == before


@pytest.mark.asyncio
@pytest.mark.parametrize('proof', ['unidentified', 'wrong-input', 'invalid-turn', 'read-failed'])
async def test_lost_ack_unsuccessful_history_proof_preserves_current_error(tmp_path, proof):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    native.error = HpcMcpError('CODEX_CONNECTION_LOST', 'synthetic lost ACK')
    native.on_add = lambda: native.consume()
    await service(store, native, now).deliver_once()
    journal = store.diagnostics(event.id)['attempts']
    item = native.turns[0]['items'][0]
    if proof == 'unidentified':
        del item['clientId']
    elif proof == 'wrong-input':
        item['content'][0]['text'] = 'different message'
    elif proof == 'invalid-turn':
        native.turns[0]['id'] = ''
    else:
        def fail_read():
            raise HpcMcpError('CODEX_REQUEST_FAILED', 'synthetic read failure')
        native.on_queue = fail_read
    now[0] = 221
    await service(store, native, now).observe_once()
    result = store.events()[0]
    assert result.last_error == 'CODEX_CONNECTION_LOST'
    assert result.delivered_at is None and result.handoff_finished_at is None
    assert result.send_started_at == 100
    assert store.diagnostics(event.id)['attempts'] == journal
    assert store.claim_event('replay', now=1000) is None
    assert [m for m, _ in native.calls].count('thread/queue/add') == 1
    no_direct(native)


def test_cursor_schema_rejects_integer_only_pagination_preflight():
    from hpc_mcp.agent.queue_protocol import require_queue_schema
    schema = queue_schema()
    schema.methods['thread/queue/list']['properties']['cursor'] = {'type': 'integer'}
    with pytest.raises(HpcMcpError):
        require_queue_schema(schema)


@pytest.mark.asyncio
async def test_same_queue_history_overlap_defers_before_conflict(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    queued = copy.deepcopy(native.queue)
    native.consume()
    native.queue = queued
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    assert store.events()[0].delivered_at is None
    native.queue.clear()
    now[0] = 202
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'COMPLETED'


@pytest.mark.asyncio
async def test_writer_refusal_preserves_exact_queued_evidence(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    await service(store, native, now).deliver_once()
    def refusal():
        raise HpcMcpError('CODEX_REQUEST_FAILED', 'secret writer refusal')
    native.on_resume = refusal
    now[0] = 151
    svc = service(store, native, now)
    await svc.observe_once()
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    assert not svc._owned_sessions
    assert QueueStore(store).get(event.id).last_error == 'CODEX_REQUEST_FAILED'
    assert native.closed == native.opened
    no_direct(native)


@pytest.mark.asyncio
async def test_changed_persisted_queue_id_conflicts_before_resume(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    native.queue[0]['id'] = 'replacement-id'
    now[0] = 151
    try:
        await svc.observe_once()
        assert QueueStore(store).get(event.id).state == 'CONFLICT'
        assert QueueStore(store).get(event.id).queue_id == 'queued-id'
        assert store.events()[0].delivery_state == 'MANUAL_REQUIRED'
        assert not any(m == 'thread/resume' for m, _ in native.calls)
    finally:
        await svc.aclose()
    assert native.closed == native.opened
    no_direct(native)


@pytest.mark.asyncio
async def test_shutdown_during_owned_work_preserves_restart_receipt(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    svc = service(store, native, now, observation_interval=.01)
    await svc.deliver_once()
    native.on_resume = lambda: native.consume('interrupted', None)
    now[0] = 151
    await svc.observe_once()
    await asyncio.sleep(.02)
    await svc.aclose()
    assert native.closed == native.opened
    assert QueueStore(store).get(event.id).state == 'OBSERVING'
    assert store.events()[0].handoff_state == 'UNKNOWN'
    assert store.events()[0].handoff_finished_at is None
    native.turns[0].update(status='completed', completedAt=200)
    now[0] = 202
    await service(type(store)(store.path), native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'COMPLETED'
    assert [m for m, _ in native.calls].count('thread/queue/add') == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('conflict', ['duplicate-queue', 'duplicate-history', 'wrong-input'])
async def test_conflicting_identity_stops_observation(tmp_path, conflict):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    if conflict == 'duplicate-queue':
        native.queue.append(dict(native.queue[0], id='second'))
    elif conflict == 'duplicate-history':
        native.consume()
        native.turns[0]['items'].append(dict(native.turns[0]['items'][0], id='second'))
    else:
        native.queue[0]['input'][0]['text'] = 'wrong'
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'CONFLICT'
    assert store.events()[0].delivery_state == 'MANUAL_REQUIRED'
    assert QueueStore(store).claim_observation('later', now=1000) is None


@pytest.mark.asyncio
async def test_bounded_batch_alternates_observation_and_new_work(tmp_path):
    store, _, _ = setup_event(tmp_path)
    svc = service(store, Native())
    calls = []
    async def observe(**kwargs):
        calls.append('observe')
        return True
    async def deliver():
        calls.append('deliver')
        return True
    svc.observe_once, svc.deliver_once = observe, deliver
    await svc.deliver_pending(limit=4)
    await svc.wait_idle()
    assert calls == ['observe', 'deliver', 'observe', 'deliver']


@pytest.mark.asyncio
async def test_admission_cancellation_keeps_intent_and_cleans(tmp_path):
    store, _, event = setup_event(tmp_path)
    native = Native()
    sent = asyncio.Event()
    original = native.request
    async def request(method, params):
        if method == 'thread/queue/add':
            sent.set()
            await asyncio.Event().wait()
        return await original(method, params)
    native.request = request
    svc = service(store, native)
    task = asyncio.create_task(svc.deliver_once())
    await sent.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert QueueStore(store).get(event.id).state == 'INTENT'
    assert store.events()[0].send_started_at == 100
    assert native.closed == native.opened
    assert store.diagnostics(event.id)['attempts'][0]['cleanup_state'] == 'CONFIRMED'


@pytest.mark.asyncio
async def test_full_scan_once_and_exact_paginated_target(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    native.consume('interrupted', None)
    native.pages = [{'data': [{'turnId': 'target', 'item': native.turns[0]['items'][0]}], 'nextCursor': None}]
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'OBSERVING'
    assert [m for m, _ in native.calls].count('thread/items/list') == 1
    assert [p for m, p in native.calls if m == 'thread/turns/list'][0]['limit'] == 20


def test_queue_manual_recovery_uses_persisted_renderer(tmp_path):
    store, watch, event = setup_event(tmp_path)
    leased = store.claim_event('one', now=10)
    from hpc_mcp.agent.queue_protocol import context_digest, message_digest
    message = render_queue_message(watch, event)
    QueueStore(store).begin_intent(leased, 'one', 2, message_digest(message), context_digest(watch, event), now=11)
    result = legacy.manual_recovery(store, event.id)
    assert result['message'] == message
    assert result['queue']['state'] == 'INTENT'
    assert 'requeue' in result['instructions']


@pytest.mark.asyncio
async def test_owned_capacity_blocks_more_admission(tmp_path):
    store, _, event = setup_event(tmp_path)
    svc = service(store, Native())
    svc._owned_sessions = {object(): None for _ in range(20)}
    assert not await svc.deliver_once()
    assert store.events()[0].attempt_count == 0
    assert QueueStore(store).get(event.id) is None


@pytest.mark.asyncio
async def test_observer_cleanup_saturation_backpressures_then_recovers(tmp_path, monkeypatch):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    close = svc._close_manager
    async def unconfirmed(manager):
        return False
    monkeypatch.setattr(svc, '_close_manager', unconfirmed)
    try:
        for _ in range(20):
            now[0] += 51
            assert await svc.observe_once()
        assert len(svc._owned_sessions) == 20
        assert native.opened - native.closed == 20
        before = QueueStore(store).get(event.id)
        for _ in range(3):
            now[0] += 51
            assert not await svc.observe_once()
            assert len(svc._owned_sessions) == 20
            assert native.opened - native.closed == 20
            assert QueueStore(store).get(event.id) == before
        native.consume()
        monkeypatch.setattr(svc, '_close_manager', close)
        assert await svc.observe_once()
        assert QueueStore(store).get(event.id).state == 'COMPLETED'
        assert len(svc._owned_sessions) == 19
        assert event.id in svc._owned_events  # Other cleanup children still belong to this event.
    finally:
        monkeypatch.setattr(svc, '_close_manager', close)
        await svc.aclose()
    assert native.opened == native.closed


@pytest.mark.asyncio
async def test_saturated_owners_reserve_reader_and_reach_final_proof(tmp_path):
    store, _, first = setup_event(tmp_path)
    events = [first] + [setup_event(tmp_path, job=str(200 + n))[2] for n in range(19)]
    native, now = Native('notLoaded'), [100]
    native.on_add = lambda: native.queue[-1].update(id='queue-' + native.queue[-1]['clientUserMessageId'])
    svc = service(store, native, now, observation_interval=.005)
    peak = 0
    factory = svc.client_factory
    @asynccontextmanager
    async def counted_factory(schema):
        nonlocal peak
        async with factory(schema) as client:
            peak = max(peak, native.opened - native.closed)
            yield client
    svc.client_factory = counted_factory
    try:
        for _ in events:
            assert await svc.deliver_once()
        now[0] = 151
        for event in events:
            assert await svc.observe_once(event_id=event.id)
        assert len(svc._owned_sessions) == 19
        assert [m for m, _ in native.calls].count('thread/resume') == 19
        _, _, pending = setup_event(tmp_path, job='999')
        assert not await svc.deliver_once()
        assert QueueStore(store).get(pending.id) is None
        while native.queue:
            native.consume()
            native.turns[-1]['id'] = 'turn-' + native.turns[-1]['items'][0]['clientId']
            native.turns[-1]['items'][0]['id'] = 'input-' + native.turns[-1]['items'][0]['clientId']
        now[0] = 202
        for event in events:
            await svc.observe_once(event_id=event.id)
        await asyncio.wait_for(svc.wait_idle(), 2)
        assert all(QueueStore(store).get(e.id).state == 'COMPLETED' for e in events)
        assert not svc._owned_sessions
        assert peak == 20
    finally:
        await svc.aclose()
    assert native.opened == native.closed
    no_direct(native)


@pytest.mark.asyncio
async def test_concurrent_observers_share_reserved_reader_without_claiming_lease(tmp_path):
    store, _, first = setup_event(tmp_path)
    _, _, second = setup_event(tmp_path, job='124')
    native, now = Native(), [100]
    native.on_add = lambda: native.queue[-1].update(id='queue-' + native.queue[-1]['clientUserMessageId'])
    svc = service(store, native, now)
    await svc.deliver_once()
    await svc.deliver_once()
    now[0] = 151
    entered, release = asyncio.Event(), asyncio.Event()
    request = native.request
    async def blocked(method, params):
        if method == 'thread/queue/list':
            entered.set()
            await release.wait()
        return await request(method, params)
    native.request = blocked
    task = asyncio.create_task(svc.observe_once(event_id=first.id))
    try:
        await entered.wait()
        before = QueueStore(store).get(second.id)
        result = await asyncio.wait_for(svc.observe_once(event_id=second.id), .1)
        assert result is False
        assert QueueStore(store).get(second.id) == before
        assert native.opened - native.closed == 1
        _, _, pending = setup_event(tmp_path, job='125')
        assert not await svc.deliver_once()
        assert next(e for e in store.events() if e.id == pending.id).attempt_count == 0
    finally:
        release.set()
        await task
        await svc.aclose()


@pytest.mark.asyncio
async def test_saturated_cleanup_cancellation_keeps_child_and_does_not_claim(tmp_path, monkeypatch):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    close = svc._close_manager
    async def unconfirmed(manager):
        return False
    monkeypatch.setattr(svc, '_close_manager', unconfirmed)
    for _ in range(20):
        now[0] += 51
        await svc.observe_once()
    entered, release = asyncio.Event(), asyncio.Event()
    async def blocked_cleanup(manager):
        entered.set()
        await release.wait()
        return False
    monkeypatch.setattr(svc, '_close_manager', blocked_cleanup)
    before = QueueStore(store).get(event.id)
    task = asyncio.create_task(svc.observe_once())
    try:
        await entered.wait()
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        assert not await svc.observe_once()
        assert len(svc._owned_sessions) == 20
        assert native.opened - native.closed == 20
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert QueueStore(store).get(event.id) == before
        assert len(svc._owned_sessions) == 20
        assert not svc._operation_lock.locked()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        monkeypatch.setattr(svc, '_close_manager', close)
        await svc.aclose()
    assert native.opened == native.closed


@pytest.mark.asyncio
async def test_one_unreapable_cleanup_child_does_not_starve_other_cleanup(tmp_path, monkeypatch):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    close = svc._close_manager
    async def unconfirmed(manager):
        return False
    monkeypatch.setattr(svc, '_close_manager', unconfirmed)
    for _ in range(20):
        now[0] += 51
        await svc.observe_once()
    unreapable = next(iter(svc._owned_sessions.values()))[0]
    async def close_others(manager):
        return False if manager is unreapable else await close(manager)
    monkeypatch.setattr(svc, '_close_manager', close_others)
    try:
        native.consume()
        now[0] += 51
        assert not await svc.observe_once()
        assert await svc.observe_once()
        assert QueueStore(store).get(event.id).state == 'COMPLETED'
        assert any(manager is unreapable for manager, _, _, _ in svc._owned_sessions.values())
        assert native.opened - native.closed == 19
    finally:
        monkeypatch.setattr(svc, '_close_manager', close)
        await svc.aclose()
    assert native.opened == native.closed


@pytest.mark.asyncio
async def test_unexpected_preflight_evidence_never_adopts_or_adds(tmp_path):
    store, watch, event = setup_event(tmp_path)
    native = Native()
    native.queue = [{'id': 'external', 'clientUserMessageId': event.id,
        'input': [{'type': 'text', 'text': render_queue_message(watch, event), 'text_elements': []}]}]
    await service(store, native).deliver_once()
    assert store.events()[0].delivery_state == 'MANUAL_REQUIRED'
    assert QueueStore(store).get(event.id) is None
    assert not any(m == 'thread/queue/add' for m, _ in native.calls)


@pytest.mark.asyncio
async def test_crash_before_send_intent_is_observed_without_add(tmp_path):
    store, watch, event = setup_event(tmp_path)
    leased = store.claim_event('crashed', now=2)
    from hpc_mcp.agent.queue_protocol import context_digest, message_digest
    message = render_queue_message(watch, event)
    QueueStore(store).begin_intent(leased, 'crashed', 2, message_digest(message), context_digest(watch, event), now=3)
    native = Native()
    now = [200]
    svc = service(type(store)(store.path), native, now)
    await svc.deliver_pending()
    await svc.wait_idle()
    assert QueueStore(store).get(event.id).state == 'INTENT'
    assert not any(m == 'thread/queue/add' for m, _ in native.calls)


@pytest.mark.asyncio
async def test_active_owner_continues_after_notifier_exit(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('active'), [100]
    await service(store, native, now).deliver_once()
    assert native.closed == native.opened
    async def complete():
        await asyncio.sleep(.02)
        native.consume()
    await complete()
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'COMPLETED'
    assert not any(m == 'thread/resume' for m, _ in native.calls)


@pytest.mark.asyncio
async def test_persistent_overlap_becomes_manual_conflict(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    queued = copy.deepcopy(native.queue)
    native.consume()
    native.queue = queued
    svc = service(store, native, now)
    now[0] = 151
    await svc.observe_once()
    now[0] = 202
    await svc.observe_once()
    assert QueueStore(store).get(event.id).state == 'CONFLICT'


@pytest.mark.asyncio
async def test_owned_bounded_failure_is_unresolved_with_safe_diagnostic(tmp_path, caplog):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    svc = service(store, native, now, ownership_timeout=.02, observation_interval=.01)
    await svc.deliver_once()
    now[0] = 151
    await svc.observe_once()
    await svc.wait_idle()
    assert QueueStore(store).get(event.id).state == 'QUEUED'
    assert QueueStore(store).get(event.id).last_error == 'CODEX_SESSION_INTERRUPTED'
    assert store.events()[0].delivered_at is None
    assert native.closed == native.opened
    assert 'private secret' not in caplog.text


@pytest.mark.asyncio
async def test_owner_diagnostic_persistence_error_cannot_skip_child_cleanup(tmp_path, monkeypatch, caplog):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded'), [100]
    svc = service(store, native, now, ownership_timeout=.02, observation_interval=.01)
    await svc.deliver_once()
    def fail(*args, **kwargs):
        raise RuntimeError('private storage detail')
    monkeypatch.setattr(svc.queue, 'record_owner_stop', fail)
    now[0] = 151
    await svc.observe_once()
    await svc.wait_idle()
    assert native.closed == native.opened
    assert not svc._owned_sessions
    assert 'private storage detail' not in caplog.text


@pytest.mark.asyncio
@pytest.mark.parametrize('column', ['message_version', 'format_version'])
async def test_unknown_persisted_renderer_is_manual_without_native_calls(tmp_path, column):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    with store._connect() as db:
        db.execute(f'UPDATE queue_handoffs SET {column}=99 WHERE event_id=?', (event.id,))
    calls = len(native.calls)
    now[0] = 151
    await service(store, native, now).observe_once()
    assert QueueStore(store).get(event.id).state == 'CONFLICT'
    assert store.events()[0].delivery_state == 'MANUAL_REQUIRED'
    assert len(native.calls) == calls


@pytest.mark.asyncio
async def test_observation_unconfirmed_cleanup_retained_and_visible(tmp_path, monkeypatch):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    await service(store, native, now).deliver_once()
    svc = service(store, native, now)
    async def unconfirmed(manager):
        return False
    monkeypatch.setattr(svc, '_close_manager', unconfirmed)
    now[0] = 151
    await svc.observe_once()
    assert svc._owned_sessions
    assert store.events()[0].handoff_error == 'CODEX_CHILD_CLEANUP_UNCONFIRMED'
    assert QueueStore(store).get(event.id).last_error == 'CODEX_CHILD_CLEANUP_UNCONFIRMED'


@pytest.mark.asyncio
async def test_incompatible_schema_leaves_never_sent_pending(tmp_path):
    store, _, event = setup_event(tmp_path)
    native = Native()
    from tests.unit.test_app_server import contract
    async def old_schema():
        return contract()
    svc = service(store, native)
    svc.schema_loader = old_schema
    await svc.deliver_once()
    assert store.events()[0].delivery_state == 'PENDING'
    assert store.events()[0].last_error == 'CODEX_PROTOCOL_INCOMPATIBLE'
    assert QueueStore(store).get(event.id) is None
    assert not native.calls


@pytest.mark.asyncio
async def test_passive_close_cannot_change_observed_running_outcome(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native('active'), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    native.consume('interrupted', None)
    now[0] = 151
    await svc.observe_once()
    await svc.aclose()
    assert not svc._owned_sessions
    assert store.events()[0].handoff_state == 'RUNNING'
    assert store.events()[0].handoff_finished_at is None
    assert QueueStore(store).get(event.id).last_error is None


@pytest.mark.asyncio
async def test_owned_stop_diagnostic_cannot_overwrite_verified_final_proof(tmp_path):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    native.consume()
    now[0] = 151
    await svc.observe_once()
    before = store.events()[0]
    queue = QueueStore(store)
    assert not queue.record_owner_stop(queue.get(event.id), now=160, error_code='CODEX_SESSION_INTERRUPTED')
    assert store.events()[0] == before
    assert queue.get(event.id).state == 'COMPLETED'


@pytest.mark.asyncio
async def test_cleanup_only_retained_observer_is_not_a_resumed_owner(tmp_path, monkeypatch):
    store, _, event = setup_event(tmp_path)
    native, now = Native(), [100]
    svc = service(store, native, now)
    await svc.deliver_once()
    native.consume('interrupted', None)
    close = svc._close_manager
    async def unconfirmed(manager):
        return False
    monkeypatch.setattr(svc, '_close_manager', unconfirmed)
    now[0] = 151
    await svc.observe_once()
    assert svc._owned_sessions
    monkeypatch.setattr(svc, '_close_manager', close)
    await svc.aclose()
    assert not svc._owned_sessions
    assert store.events()[0].handoff_state == 'RUNNING'


@pytest.mark.asyncio
@pytest.mark.parametrize('owned', [True, False])
async def test_conflict_stopped_owner_is_unknown_but_passive_close_preserves_running(tmp_path, owned):
    store, _, event = setup_event(tmp_path)
    native, now = Native('notLoaded' if owned else 'active'), [100]
    svc = service(store, native, now, observation_interval=.005)
    try:
        await svc.deliver_once()
        if owned:
            native.on_resume = lambda: native.consume('interrupted', None)
        else:
            native.consume('interrupted', None)
        now[0] = 151
        await svc.observe_once()
        if owned:
            for _ in range(100):
                if store.events()[0].handoff_state == 'RUNNING':
                    break
                await asyncio.sleep(.005)
        assert store.events()[0].handoff_state == 'RUNNING'
        assert store.events()[0].delivery_state == 'DELIVERED'
        native.turns[0]['items'].append(dict(native.turns[0]['items'][0], id='duplicate'))
        now[0] = 202
        await svc.observe_once()
        await asyncio.wait_for(svc.wait_idle(), 1)
        assert QueueStore(store).get(event.id).state == 'CONFLICT'
        assert store.events()[0].delivery_state == 'MANUAL_REQUIRED'
        assert store.events()[0].handoff_state == ('UNKNOWN' if owned else 'RUNNING')
        assert store.events()[0].handoff_finished_at is None
        assert store.events()[0].handoff_error == 'CODEX_QUEUE_CONFLICT'
        assert not svc._owned_sessions
        assert native.opened == native.closed
    finally:
        await svc.aclose()
