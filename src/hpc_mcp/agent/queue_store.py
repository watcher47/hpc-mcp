"""Durable one-shot queue intent and independently fenced observations."""
from dataclasses import dataclass
import math
import re

from .store import _finite_times, safe_delivery_code

FINAL = frozenset({'COMPLETED', 'FAILED', 'INTERRUPTED', 'CONFLICT'})
REPROOF_TRANSIENT_ERRORS = frozenset({
    'CODEX_UNAVAILABLE', 'CODEX_TIMEOUT', 'CODEX_CONNECTION_LOST',
    'CODEX_REQUEST_FAILED', 'CODEX_SESSION_INTERRUPTED',
})

# Revisit only the historical Desktop identity failure, without reopening send
# authorization or resetting its state. Successful proof still needs the lease.
_REVISIT = f"""state='CONFLICT' AND (last_error='CODEX_HISTORY_IDENTITY_UNAVAILABLE'
    OR (last_error IN ({','.join(repr(code) for code in sorted(REPROOF_TRANSIENT_ERRORS))})
        AND EXISTS (SELECT 1 FROM queue_reproof_origins o WHERE o.event_id=queue_handoffs.event_id
            AND o.origin_error='CODEX_HISTORY_IDENTITY_UNAVAILABLE')))
    AND format_version=1 AND message_version=2 AND acknowledged_at>=intent_at
    AND queue_id IS NOT NULL AND length(queue_id) BETWEEN 1 AND 256 AND turn_id IS NULL
    AND NOT EXISTS (SELECT 1 FROM queue_input_bindings b WHERE b.event_id=queue_handoffs.event_id)
    AND EXISTS (SELECT 1 FROM terminal_events e WHERE e.id=queue_handoffs.event_id
        AND e.delivery_state='MANUAL_REQUIRED' AND e.handoff_state='NONE'
        AND e.turn_id IS NULL AND e.delivered_at IS NULL AND e.send_started_at IS NOT NULL)"""


def _identity(value):
    return isinstance(value, str) and 0 < len(value) <= 256 and not any(ord(c) < 32 for c in value)


@dataclass(frozen=True)
class QueueReceipt:
    event_id: str
    format_version: int
    message_version: int
    message_sha256: str
    context_sha256: str
    state: str
    queue_id: str | None
    turn_id: str | None
    intent_at: float
    acknowledged_at: float | None
    observed_at: float | None
    next_observe_at: float
    lease_owner: str | None
    lease_until: float | None
    revision: int
    last_error: str | None
    item_id: str | None = None
    identity_kind: str | None = None
    prior_identity_error: str | None = None

    @property
    def has_confirmed_ack(self):
        return (self.format_version == 1 and self.message_version == 2
            and _identity(self.queue_id) and type(self.acknowledged_at) in (int, float)
            and math.isfinite(self.acknowledged_at) and self.acknowledged_at >= self.intent_at)


class QueueStore:
    def __init__(self, agent_store):
        self.store = agent_store

    def get(self, event_id):
        with self.store._connect() as db:
            row = db.execute('''SELECT q.*, b.item_id,b.identity_kind,
                COALESCE(b.prior_identity_error,o.origin_error) AS prior_identity_error
                FROM queue_handoffs q LEFT JOIN queue_input_bindings b ON b.event_id=q.event_id
                LEFT JOIN queue_reproof_origins o ON o.event_id=q.event_id
                WHERE q.event_id=?''', (event_id,)).fetchone()
        return QueueReceipt(**dict(row)) if row else None

    def receipts(self):
        with self.store._connect() as db:
            return [QueueReceipt(**dict(row)) for row in db.execute('''SELECT q.*,
                b.item_id,b.identity_kind,
                COALESCE(b.prior_identity_error,o.origin_error) AS prior_identity_error FROM queue_handoffs q
                LEFT JOIN queue_input_bindings b ON b.event_id=q.event_id
                LEFT JOIN queue_reproof_origins o ON o.event_id=q.event_id ORDER BY intent_at,q.event_id''')]

    def record_owner_stop(self, receipt, *, now, error_code):
        """Persist fenced owner-stop uncertainty, never terminal proof."""
        _finite_times(now)
        with self.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("""UPDATE queue_handoffs SET last_error=COALESCE(last_error,?),
                revision=revision+1 WHERE event_id=? AND revision=? AND format_version=?
                AND message_version=? AND message_sha256=? AND context_sha256=?
                AND (lease_until IS NULL OR lease_until<=?)
                AND state NOT IN ('COMPLETED','FAILED','INTERRUPTED')""",
                (safe_delivery_code(error_code), receipt.event_id, receipt.revision,
                 receipt.format_version, receipt.message_version, receipt.message_sha256,
                 receipt.context_sha256, now)).rowcount == 1
            if changed and receipt.turn_id:
                db.execute("""UPDATE terminal_events SET handoff_state='UNKNOWN',
                    handoff_error=COALESCE(handoff_error,?)
                    WHERE id=? AND turn_id=? AND (delivery_state='DELIVERED'
                    OR (delivery_state='MANUAL_REQUIRED' AND ?='CONFLICT'))
                    AND handoff_state='RUNNING'""", (safe_delivery_code(error_code),
                    receipt.event_id, receipt.turn_id, receipt.state))
            return changed

    def begin_intent(self, event, owner, message_version, message_sha256, context_sha256, *, now):
        _finite_times(now)
        if (not _identity(owner) or message_version != 2 or any(not isinstance(d, str)
                or not re.fullmatch('[0-9a-f]{64}', d) for d in (message_sha256, context_sha256))):
            raise ValueError('Invalid queue intent identity')
        with self.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("""UPDATE terminal_events SET send_started_at=? WHERE id=?
                AND lease_owner=? AND lease_until>? AND delivery_state='IN_FLIGHT'
                AND send_started_at IS NULL AND NOT EXISTS
                (SELECT 1 FROM queue_handoffs WHERE event_id=terminal_events.id)""",
                (now, event.id, owner, now)).rowcount
            if not changed:
                return None
            db.execute("""INSERT INTO queue_handoffs
                (event_id,message_version,message_sha256,context_sha256,state,intent_at,next_observe_at)
                VALUES (?,?,?,?,'INTENT',?,?)""",
                (event.id, message_version, message_sha256, context_sha256, now, now))
        return self.get(event.id)

    def record_ack(self, receipt, owner, queue_id, *, now):
        _finite_times(now)
        if not _identity(queue_id):
            raise ValueError('Invalid queue identity')
        with self.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            changed = db.execute("""UPDATE queue_handoffs SET queue_id=?,state='QUEUED',acknowledged_at=?
                WHERE event_id=? AND revision=? AND state='INTENT' AND lease_owner IS NULL
                AND format_version=? AND message_version=? AND message_sha256=? AND context_sha256=? AND EXISTS
                (SELECT 1 FROM terminal_events e WHERE e.id=event_id AND e.lease_owner=?
                 AND e.lease_until>? AND e.delivery_state='IN_FLIGHT')""",
                (queue_id, now, receipt.event_id, receipt.revision, receipt.format_version, receipt.message_version, receipt.message_sha256,
                 receipt.context_sha256, owner, now)).rowcount
            if changed:
                db.execute("""UPDATE terminal_events SET delivery_state='QUEUED',lease_owner=NULL,
                    lease_until=NULL WHERE id=?""", (receipt.event_id,))
                self.store._finish_attempt(db, receipt.event_id, owner, now, 'QUEUED')
        return bool(changed)

    def claim_observation(self, owner, *, now, lease_seconds=120, event_id=None):
        _finite_times(now, lease_seconds, now + lease_seconds)
        if not _identity(owner) or not 0 < lease_seconds <= 120:
            raise ValueError('Invalid observation lease')
        with self.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            row = db.execute(f"""SELECT * FROM queue_handoffs WHERE (state NOT IN
                ('COMPLETED','FAILED','INTERRUPTED','CONFLICT') OR ({_REVISIT})) AND next_observe_at<=?
                AND (? IS NULL OR event_id=?)
                AND (lease_until IS NULL OR lease_until<=?) AND NOT EXISTS
                (SELECT 1 FROM terminal_events e WHERE e.id=event_id AND e.lease_until>?)
                ORDER BY next_observe_at,intent_at,event_id LIMIT 1""", (now, event_id, event_id, now, now)).fetchone()
            if row is None:
                return None
            db.execute("""UPDATE queue_handoffs SET lease_owner=?,lease_until=?,revision=revision+1
                WHERE event_id=?""", (owner, now + lease_seconds, row['event_id']))
            if row['state'] == 'CONFLICT' and row['last_error'] == 'CODEX_HISTORY_IDENTITY_UNAVAILABLE':
                # Capture provenance only in the eligible claim transaction,
                # before any external read can replace the latest error.
                db.execute('''INSERT OR IGNORE INTO queue_reproof_origins
                    (event_id,origin_error,created_at) VALUES (?,?,?)''',
                    (row['event_id'], row['last_error'], now))
        return self.get(row['event_id'])

    def record_observation(self, receipt, owner, *, now, state=None, queue_id=None,
                           turn_id=None, item_id=None, identity_kind=None,
                           next_observe_at=None, error_code=None):
        next_observe_at = now + 50 if next_observe_at is None else next_observe_at
        _finite_times(now, next_observe_at)
        preserve = state is None
        state = receipt.state if preserve else state
        if state not in FINAL | {'INTENT', 'QUEUED', 'OBSERVING'}:
            raise ValueError('Invalid queue state')
        if any(value is not None and not _identity(value) for value in (queue_id, turn_id, item_id)):
            raise ValueError('Invalid evidence identity')
        if (item_id is None) != (identity_kind is None) or (identity_kind is not None
                and identity_kind not in {'NATIVE_CLIENT_ID', 'ACK_MESSAGE_NATIVE_ITEM'}):
            raise ValueError('Invalid input evidence kind')
        if identity_kind == 'ACK_MESSAGE_NATIVE_ITEM' and not receipt.has_confirmed_ack:
            raise ValueError('Confirmed queue ACK required')
        if state in {'OBSERVING', 'COMPLETED', 'FAILED', 'INTERRUPTED'} and not (turn_id or (preserve and receipt.turn_id)):
            raise ValueError('Exact turn evidence required')
        if state == 'QUEUED' and not (queue_id or receipt.queue_id):
            raise ValueError('Exact queue evidence required')
        if ((receipt.queue_id and queue_id and receipt.queue_id != queue_id)
                or (receipt.turn_id and turn_id and receipt.turn_id != turn_id)
                or (receipt.item_id and item_id and receipt.item_id != item_id)):
            state, error_code = 'CONFLICT', 'CODEX_QUEUE_CONFLICT'
        # A conflict claim authorizes reads only. Neither queue presence nor
        # absence is permission to resume or to clear this fence.
        if receipt.state == 'CONFLICT' and not (state in {'OBSERVING','COMPLETED','FAILED','INTERRUPTED'}
                and item_id and identity_kind == 'ACK_MESSAGE_NATIVE_ITEM'):
            state = 'CONFLICT'
        with self.store._connect() as db:
            db.execute('BEGIN IMMEDIATE')
            binding = db.execute('SELECT * FROM queue_input_bindings WHERE event_id=?',
                (receipt.event_id,)).fetchone()
            if binding and item_id and (binding['item_id'] != item_id
                    or binding['bound_turn_id'] != turn_id
                    or binding['message_sha256'] != receipt.message_sha256
                    or binding['context_sha256'] != receipt.context_sha256):
                state, error_code = 'CONFLICT', 'CODEX_QUEUE_CONFLICT'
            changed = db.execute(f"""UPDATE queue_handoffs SET state=?,queue_id=COALESCE(queue_id,?),
                turn_id=COALESCE(turn_id,?),observed_at=?,next_observe_at=?,lease_owner=NULL,
                lease_until=NULL,last_error=? WHERE event_id=? AND lease_owner=? AND lease_until>?
                AND revision=? AND (format_version=1 OR ?='CONFLICT') AND format_version=? AND message_version=?
                AND message_sha256=? AND context_sha256=?
                AND (state NOT IN ('COMPLETED','FAILED','INTERRUPTED','CONFLICT') OR ({_REVISIT}))""",
                (state, queue_id, turn_id, now, next_observe_at,
                 safe_delivery_code(error_code) if error_code else None, receipt.event_id, owner,
                 now, receipt.revision, state, receipt.format_version, receipt.message_version,
                 receipt.message_sha256, receipt.context_sha256)).rowcount
            if changed:
                if state in {'OBSERVING', 'COMPLETED', 'FAILED', 'INTERRUPTED'} and not preserve:
                    if item_id:
                        db.execute('''INSERT OR IGNORE INTO queue_input_bindings
                            (event_id,item_id,bound_turn_id,identity_kind,message_sha256,context_sha256,
                             bound_at,prior_identity_error) VALUES (?,?,?,?,?,?,?,
                                (SELECT origin_error FROM queue_reproof_origins WHERE event_id=?))''',
                            (receipt.event_id,item_id,turn_id,identity_kind,receipt.message_sha256,
                             receipt.context_sha256,now,receipt.event_id))
                    db.execute("""UPDATE terminal_events SET delivery_state='DELIVERED',
                        delivered_at=COALESCE(delivered_at,?),turn_id=?,handoff_state=?,
                        handoff_finished_at=?,handoff_error=NULL,last_error=NULL,
                        lease_owner=NULL,lease_until=NULL
                        WHERE id=?""", (now, turn_id, 'RUNNING' if state == 'OBSERVING' else state,
                        None if state == 'OBSERVING' else now, receipt.event_id))
                elif state == 'QUEUED':
                    db.execute("""UPDATE terminal_events SET delivery_state='QUEUED',
                        lease_owner=NULL,lease_until=NULL WHERE id=? AND delivery_state!='DELIVERED'""",
                        (receipt.event_id,))
                elif state == 'CONFLICT':
                    db.execute("""UPDATE terminal_events SET delivery_state='MANUAL_REQUIRED',
                        handoff_error='CODEX_QUEUE_CONFLICT',lease_owner=NULL,lease_until=NULL WHERE id=?""",
                        (receipt.event_id,))
        return bool(changed)
