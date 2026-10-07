import sqlite3

from hpc_mcp.agent.store import AgentStore
import importlib
from dataclasses import replace
import pytest


def fixture(tmp_path):
    store = AgentStore(tmp_path / 'state.db')
    watch = store.register_watch(server_alias='fixture', job_id='12', project_id='project',
        thread_id='thread', session_id='session', cwd=str(tmp_path))
    store.record_terminal(watch.id, 'COMPLETED', now=1)
    event = store.claim_event('worker', now=2)
    assert importlib.util.find_spec('hpc_mcp.agent.queue_store') is not None, 'durable queue API missing'
    return store, event, importlib.import_module('hpc_mcp.agent.queue_store').QueueStore(store)


def test_atomic_one_shot_and_compatible_table(tmp_path):
    store, event, queue = fixture(tmp_path)
    before = set(vars(event))
    receipt = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert receipt.state == 'INTENT'
    assert not queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert store.events()[0].send_started_at == 3
    assert set(vars(store.events()[0])) == before
    assert store.claim_event('other', now=200) is None
    with sqlite3.connect(store.path) as db:
        assert db.execute('PRAGMA user_version').fetchone()[0] == 3


def test_stale_intent_ack_and_observation_are_fenced(tmp_path):
    store, event, queue = fixture(tmp_path)
    assert queue.begin_intent(event, 'other', 2, 'a'*64, 'b'*64, now=3) is None
    receipt = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(receipt, 'worker', 'queue', now=123) is False
    claim = queue.claim_observation('observer', now=124)
    assert claim.revision == 1
    assert queue.claim_observation('other', now=125) is None
    newer = queue.claim_observation('other', now=245)
    assert not queue.record_observation(claim, 'observer', now=246, state='QUEUED', queue_id='queue')
    assert queue.record_observation(newer, 'other', now=246, state='OBSERVING', turn_id='turn')
    assert store.events()[0].delivery_state == 'DELIVERED'
    store.recover_interrupted_handoffs(now=247)
    assert store.events()[0].handoff_state == 'RUNNING'


def test_ack_is_not_delivery_and_exact_final_observation(tmp_path):
    store, event, queue = fixture(tmp_path)
    receipt = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(receipt, 'worker', 'queue', now=4)
    assert store.events()[0].delivery_state == 'QUEUED'
    assert store.events()[0].delivered_at is None
    claim = queue.claim_observation('observer', now=5)
    assert queue.record_observation(claim, 'observer', now=6, state='COMPLETED', turn_id='turn')
    assert store.events()[0].handoff_state == 'COMPLETED'
    assert queue.claim_observation('observer', now=200) is None


def test_legacy_send_and_expired_lease_never_authorize_intent(tmp_path):
    store, event, queue = fixture(tmp_path)
    assert queue.begin_intent(event,'worker',2,'a'*64,'b'*64,now=122) is None
    assert store.mark_send_started(event.id,'worker',now=3)
    assert queue.begin_intent(event,'worker',2,'a'*64,'b'*64,now=4) is None


def test_old_completed_row_unchanged_by_initialization(tmp_path):
    store,event,queue=fixture(tmp_path)
    assert store.mark_delivered(event.id,'worker',now=3,turn_id='legacy-turn',handoff_state='RUNNING')
    assert store.finish_handoff(event.id,'legacy-turn','COMPLETED',now=4)
    before=store.events()
    AgentStore(store.path)
    assert store.events() == before


def test_immutable_ack_and_observation_identity(tmp_path):
    store,event,queue=fixture(tmp_path)
    receipt=queue.begin_intent(event,'worker',2,'a'*64,'b'*64,now=3)
    assert not queue.record_ack(replace(receipt,message_version=99),'worker','queue',now=4)
    assert queue.record_ack(receipt,'worker','queue',now=4)
    claim=queue.claim_observation('observer',now=5)
    assert not queue.record_observation(replace(claim,message_version=99),'observer',now=6,state='QUEUED',queue_id='queue')


def test_unknown_version_digest_validation_and_safe_error(tmp_path):
    store,event,queue=fixture(tmp_path)
    for version,digest in [(99,'a'*64),(2,'raw unsafe error')]:
        with pytest.raises(ValueError): queue.begin_intent(event,'worker',version,digest,'b'*64,now=3)
    receipt=queue.begin_intent(event,'worker',2,'a'*64,'b'*64,now=3)
    assert queue.record_ack(receipt,'worker','queue',now=4)
    claim=queue.claim_observation('observer',now=5)
    assert queue.record_observation(claim,'observer',now=6,error_code='raw unsafe error')
    assert queue.get(event.id).last_error == 'CODEX_HANDOFF_FAILED'


def test_observation_error_preserves_evidence_without_repairing_unknown(tmp_path):
    store,event,queue=fixture(tmp_path)
    receipt=queue.begin_intent(event,'worker',2,'a'*64,'b'*64,now=3)
    queue.record_ack(receipt,'worker','queue',now=4)
    claim=queue.claim_observation('observer',now=5)
    assert queue.record_observation(claim,'observer',now=6,state='OBSERVING',turn_id='turn')
    with store._connect() as db:
        db.execute("UPDATE terminal_events SET handoff_state='UNKNOWN'")
    claim=queue.claim_observation('observer',now=60)
    assert queue.record_observation(claim,'observer',now=61,error_code='CODEX_TIMEOUT')
    assert store.events()[0].handoff_state == 'UNKNOWN'
    claim=queue.claim_observation('observer',now=120)
    assert queue.record_observation(claim,'observer',now=121,state='OBSERVING',turn_id='turn')
    assert store.events()[0].handoff_state == 'RUNNING'


@pytest.mark.parametrize('state', ['OBSERVING', 'COMPLETED', 'FAILED', 'INTERRUPTED'])
def test_fresh_exact_delivery_clears_current_error_and_preserves_journal(tmp_path, state):
    store, event, queue = fixture(tmp_path)
    queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert store.defer_event(event.id, 'worker', now=4, retry_at=5,
        error_code='CODEX_CONNECTION_LOST')
    journal = store.diagnostics(event.id)['attempts']
    claim = queue.claim_observation('observer', now=5)
    assert queue.record_observation(claim, 'observer', now=6, state=state, turn_id='turn')
    result = store.events()[0]
    assert result.last_error is None
    assert result.handoff_finished_at == (None if state == 'OBSERVING' else 6)
    assert store.diagnostics(event.id)['attempts'] == journal
    before = store.events()
    assert not queue.record_observation(claim, 'observer', now=7, state='COMPLETED', turn_id='turn')
    assert store.events() == before


def test_rejected_exact_observation_keeps_current_error_and_send_fence(tmp_path):
    store, event, queue = fixture(tmp_path)
    queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert store.defer_event(event.id, 'worker', now=4, retry_at=5,
        error_code='CODEX_CONNECTION_LOST')
    claim = queue.claim_observation('observer', now=5)
    before = store.events()
    for receipt, owner, now in [(replace(claim, message_sha256='c'*64), 'observer', 6),
                               (claim, 'wrong-owner', 6), (claim, 'observer', 125)]:
        assert not queue.record_observation(receipt, owner, now=now,
            state='COMPLETED', turn_id='turn')
        assert store.events() == before
    assert before[0].last_error == 'CODEX_CONNECTION_LOST'
    assert store.claim_event('replay', now=1000) is None


def test_reproof_retries_timeout_after_reopen(tmp_path):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    first = queue.claim_observation('old', now=5)
    assert queue.record_observation(first, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    observer = queue.claim_observation('observer', now=57)
    assert queue.record_observation(observer, 'observer', now=58,
        error_code='CODEX_TIMEOUT')
    reopened = type(queue)(AgentStore(store.path))
    assert reopened.get(event.id).last_error == 'CODEX_TIMEOUT'
    assert reopened.claim_observation('early', now=107) is None
    retry = reopened.claim_observation('retry', now=109)
    assert retry is not None
    assert reopened.record_observation(retry, 'retry', now=110,
        state='COMPLETED', turn_id='turn', item_id='input',
        identity_kind='ACK_MESSAGE_NATIVE_ITEM')
    assert reopened.get(event.id).prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
    assert store.events()[0].handoff_state == 'COMPLETED'
    assert store.claim_event('replay', now=1000) is None


def test_desktop_conflict_reproof_requires_current_lease_and_preserves_rollback_rows(tmp_path):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('old', now=5)
    assert queue.record_observation(claim, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    claim = queue.claim_observation('observer', now=57)
    assert claim is not None
    assert queue.claim_observation('competitor', now=58) is None
    newer = queue.claim_observation('competitor', now=178)
    proof = dict(state='COMPLETED', turn_id='turn', item_id='input', identity_kind='ACK_MESSAGE_NATIVE_ITEM')
    assert not queue.record_observation(claim, 'observer', now=179, **proof)
    assert queue.record_observation(newer, 'competitor', now=179, **proof)
    before = queue.get(event.id)
    assert before.prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
    with store._connect() as db:
        row = dict(db.execute('SELECT * FROM queue_handoffs').fetchone())
        assert 'item_id' not in row  # Old SELECT * dataclass readers still work.
        assert db.execute('PRAGMA user_version').fetchone()[0] == 3
    AgentStore(store.path)
    assert queue.get(event.id) == before
    assert queue.claim_observation('observer', now=1000) is None
    assert store.claim_event('replay', now=1000) is None


def test_reproof_origin_survives_consecutive_transient_failures(tmp_path):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('old', now=5)
    assert queue.record_observation(claim, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    errors = ['CODEX_UNAVAILABLE', 'CODEX_TIMEOUT', 'CODEX_CONNECTION_LOST',
        'CODEX_REQUEST_FAILED', 'CODEX_SESSION_INTERRUPTED']
    for index, error in enumerate(errors):
        now = 57 + index * 52
        queue = type(queue)(AgentStore(store.path))
        claim = queue.claim_observation('observer', now=now)
        assert claim is not None
        assert claim.prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
        assert queue.record_observation(claim, 'observer', now=now+1, error_code=error)
        receipt = queue.get(event.id)
        assert receipt.state == 'CONFLICT' and receipt.last_error == error
        assert queue.receipts() == [receipt]
        assert store.events()[0].delivered_at is None
        with store._connect() as db:
            assert tuple(db.execute('SELECT * FROM queue_reproof_origins').fetchone()) == (
                event.id, 'CODEX_HISTORY_IDENTITY_UNAVAILABLE', 57)
    retry = queue.claim_observation('retry', now=317)
    assert queue.record_observation(retry, 'retry', now=318, state='COMPLETED',
        turn_id='turn', item_id='input', identity_kind='ACK_MESSAGE_NATIVE_ITEM')
    assert queue.get(event.id).last_error is None
    with store._connect() as db:
        assert db.execute('SELECT prior_identity_error FROM queue_input_bindings').fetchone()[0] == (
            'CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    assert store.claim_event('replay', now=1000) is None


@pytest.mark.parametrize('error', ['CODEX_QUEUE_IDENTITY_INVALID', 'CODEX_QUEUE_CONFLICT',
    'CODEX_PROTOCOL_INCOMPATIBLE', 'CODEX_PROTOCOL_INVALID', 'CODEX_HISTORY_INCOMPLETE',
    'CODEX_OUTPUT_LIMIT', 'CODEX_SEND_OUTCOME_UNKNOWN', 'UNKNOWN_ERROR'])
def test_reproof_permanent_error_after_timeout_is_not_retried(tmp_path, error):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('old', now=5)
    assert queue.record_observation(claim, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    claim = queue.claim_observation('observer', now=57)
    assert queue.record_observation(claim, 'observer', now=58, error_code='CODEX_TIMEOUT')
    claim = queue.claim_observation('retry', now=109)
    assert claim is not None
    assert queue.record_observation(claim, 'retry', now=110, error_code=error)
    queue = type(queue)(AgentStore(store.path))
    assert queue.get(event.id).prior_identity_error == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'
    assert queue.get(event.id).last_error == ('CODEX_HANDOFF_FAILED' if error == 'UNKNOWN_ERROR' else error)
    assert queue.claim_observation('later', now=1000) is None
    assert store.claim_event('replay', now=1000) is None


@pytest.mark.parametrize('after_timeout', [False, True])
def test_reproof_stale_owner_revision_and_digests_cannot_change_origin(tmp_path, after_timeout):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    old = queue.claim_observation('old', now=5)
    assert queue.record_observation(old, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    # A stale receipt must not fabricate provenance before an eligible claim.
    assert not queue.record_observation(old, 'old', now=7, error_code='CODEX_TIMEOUT')
    with store._connect() as db:
        assert db.execute('SELECT * FROM queue_reproof_origins').fetchall() == []
    claim = queue.claim_observation('observer', now=57)
    if after_timeout:
        assert queue.record_observation(claim, 'observer', now=58, error_code='CODEX_TIMEOUT')
        claim = queue.claim_observation('observer', now=109)
    proof = dict(state='COMPLETED', turn_id='turn', item_id='input', identity_kind='ACK_MESSAGE_NATIVE_ITEM')
    before = queue.get(event.id)
    for receipt, owner in [(replace(claim, message_sha256='c'*64), 'observer'),
                           (replace(claim, context_sha256='c'*64), 'observer'),
                           (replace(claim, revision=claim.revision+1), 'observer'),
                           (claim, 'wrong-owner')]:
        assert not queue.record_observation(receipt, owner, now=claim.lease_until-1, **proof)
        assert queue.get(event.id) == before
    assert not queue.record_observation(claim, 'observer', now=claim.lease_until, **proof)
    newer = queue.claim_observation('competitor', now=claim.lease_until)
    assert newer is not None
    assert not queue.record_observation(claim, 'observer', now=claim.lease_until+1, **proof)
    assert queue.record_observation(newer, 'competitor', now=claim.lease_until+1, **proof)
    with store._connect() as db:
        assert tuple(db.execute('SELECT * FROM queue_reproof_origins').fetchone()) == (
            event.id, 'CODEX_HISTORY_IDENTITY_UNAVAILABLE', 57)


@pytest.mark.parametrize('after_timeout', [False, True])
@pytest.mark.parametrize('guard', ['other-error', 'unacknowledged', 'early-ack',
    'no-queue-id', 'already-delivered', 'legacy', 'unknown-message', 'unknown-format',
    'event-turn', 'queue-turn', 'binding', 'no-send', 'handoff', 'terminal-lease'])
def test_other_conflicts_are_not_revisited(tmp_path, guard, after_timeout):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('old', now=5)
    assert queue.record_observation(claim, 'old', now=6, state='CONFLICT',
        error_code='CODEX_HISTORY_IDENTITY_UNAVAILABLE')
    if after_timeout:
        claim = queue.claim_observation('observer', now=57)
        assert queue.record_observation(claim, 'observer', now=58, error_code='CODEX_TIMEOUT')
    with store._connect() as db:
        if guard == 'other-error':
            db.execute("UPDATE queue_handoffs SET last_error='CODEX_QUEUE_CONFLICT'")
        elif guard == 'unacknowledged':
            db.execute('UPDATE queue_handoffs SET acknowledged_at=NULL')
        elif guard == 'early-ack':
            db.execute('UPDATE queue_handoffs SET acknowledged_at=2')
        elif guard == 'no-queue-id':
            db.execute('UPDATE queue_handoffs SET queue_id=NULL')
        elif guard == 'already-delivered':
            db.execute("UPDATE terminal_events SET delivered_at=1,handoff_state='RUNNING'")
        elif guard == 'legacy':
            db.execute('UPDATE queue_handoffs SET message_version=1')
        elif guard == 'unknown-message':
            db.execute('UPDATE queue_handoffs SET message_version=99')
        elif guard == 'unknown-format':
            db.execute('UPDATE queue_handoffs SET format_version=99')
        elif guard == 'event-turn':
            db.execute("UPDATE terminal_events SET turn_id='turn'")
        elif guard == 'queue-turn':
            db.execute("UPDATE queue_handoffs SET turn_id='turn'")
        elif guard == 'binding':
            db.execute('''INSERT INTO queue_input_bindings VALUES
                (?, 'input', 'turn', 'ACK_MESSAGE_NATIVE_ITEM', ?, ?, 7, NULL)''',
                (event.id, 'a'*64, 'b'*64))
        elif guard == 'no-send':
            db.execute('UPDATE terminal_events SET send_started_at=NULL')
        elif guard == 'handoff':
            db.execute("UPDATE terminal_events SET handoff_state='RUNNING'")
        elif guard == 'terminal-lease':
            db.execute("UPDATE terminal_events SET lease_owner='other',lease_until=1001")
    assert queue.claim_observation('observer', now=1000) is None
    assert store.claim_event('replay', now=1000) is None
    with store._connect() as db:
        assert db.execute('SELECT count(*) FROM queue_reproof_origins').fetchone()[0] == int(after_timeout)


def test_timeout_conflict_without_eligible_origin_stays_closed(tmp_path):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('old', now=5)
    assert queue.record_observation(claim, 'old', now=6, state='CONFLICT', error_code='CODEX_TIMEOUT')
    assert queue.claim_observation('observer', now=57) is None
    with store._connect() as db:
        assert db.execute('SELECT * FROM queue_reproof_origins').fetchall() == []


@pytest.mark.parametrize('state', ['COMPLETED', 'CONFLICT'])
def test_initialization_adds_empty_origin_table_without_rewriting_old_rows(tmp_path, state):
    store, event, queue = fixture(tmp_path)
    intent = queue.begin_intent(event, 'worker', 2, 'a'*64, 'b'*64, now=3)
    assert queue.record_ack(intent, 'worker', 'queue', now=4)
    claim = queue.claim_observation('observer', now=5)
    assert queue.record_observation(claim, 'observer', now=6, state=state,
        **({'turn_id': 'turn'} if state == 'COMPLETED' else {'error_code': 'CODEX_HISTORY_IDENTITY_UNAVAILABLE'}))
    with store._connect() as db:
        before_queue = tuple(db.execute('SELECT * FROM queue_handoffs').fetchone())
        before_event = tuple(db.execute('SELECT * FROM terminal_events').fetchone())
        db.execute('DROP TABLE queue_reproof_origins')
    AgentStore(store.path)
    with store._connect() as db:
        assert tuple(db.execute('SELECT * FROM queue_handoffs').fetchone()) == before_queue
        assert tuple(db.execute('SELECT * FROM terminal_events').fetchone()) == before_event
        assert db.execute('SELECT * FROM queue_reproof_origins').fetchall() == []
        assert db.execute('PRAGMA user_version').fetchone()[0] == 3
