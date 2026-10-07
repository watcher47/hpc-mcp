"""SQLite state for watches, server retries, and leased completion events.

Transactions persist a terminal event and deactivate its watch together. Event
consumers must hold the returned lease owner before acknowledging or deferring.
All times are UTC Unix seconds, making deadlines durable across process restarts.
"""
from __future__ import annotations

import re
import math
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from hpc_mcp.errors import HpcMcpError

TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT",
    "OUT_OF_MEMORY", "NODE_FAIL", "BOOT_FAIL", "DEADLINE", "PREEMPTED"})

ATTEMPT_STAGES = frozenset({"UNKNOWN", "CLAIM", "SCHEMA", "INITIALIZATION", "FIRST_HISTORY",
    "RESUME", "SECOND_HISTORY", "READINESS", "SEND_ACK", "ACCEPTED", "CLEANUP", "QUEUE_PREFLIGHT", "ENQUEUE", "QUEUED"})
ATTEMPT_OUTCOMES = frozenset({"RUNNING", "ABANDONED", "DEFERRED", "CANCELLED", "DELIVERED", "MANUAL_REQUIRED", "QUEUED"})
RPC_METHODS = frozenset({"initialize", "thread/read", "thread/items/list", "thread/turns/list",
    "thread/resume", "turn/start", "thread/queue/add", "thread/queue/list"})
DELIVERY_CODES = frozenset({"CODEX_PROTOCOL_INCOMPATIBLE", "CODEX_PROTOCOL_INVALID",
    "CODEX_CHILD_CLEANUP_UNCONFIRMED", "CODEX_CONNECTION_LOST", "CODEX_OUTPUT_LIMIT",
    "CODEX_REQUEST_FAILED", "CODEX_REQUIRES_USER_INPUT", "CODEX_TIMEOUT", "CODEX_UNAVAILABLE",
    "CODEX_EVENT_CONTEXT_INVALID", "CODEX_HANDOFF_FAILED", "CODEX_HISTORY_IDENTITY_UNAVAILABLE",
    "CODEX_HISTORY_INCOMPLETE", "CODEX_LEASE_EXPIRED", "CODEX_SEND_OUTCOME_UNKNOWN",
    "CODEX_SESSION_INTERRUPTED", "CODEX_THREAD_ACTIVE", "CODEX_THREAD_CONTEXT_INVALID",
    "CODEX_THREAD_UNAVAILABLE", "CODEX_AGENT_RESTARTED", "CODEX_HISTORY_ACCEPTED_OUTCOME_UNKNOWN",
    "CODEX_THREAD_NOT_READY", "CODEX_THREAD_CHANGED", "CODEX_READINESS_UNAVAILABLE",
    "CODEX_CANCELLED_BEFORE_SEND", "CODEX_CANCELLED_AFTER_SEND", "CODEX_ATTEMPT_ABANDONED",
    "CODEX_QUEUE_CONFLICT", "CODEX_QUEUE_IDENTITY_INVALID", "CODEX_QUEUE_TRANSITION_PENDING"})


def safe_delivery_code(code):
    return code if isinstance(code, str) and code in DELIVERY_CODES else "CODEX_HANDOFF_FAILED"


def safe_rpc(method, code):
    method = method if isinstance(method, str) and method in RPC_METHODS else None
    code = code if type(code) is int and -(2**31) <= code < 2**31 else None
    return method, code


def _finite_times(*values):
    if any(type(value) not in (int, float) or not math.isfinite(value) for value in values):
        raise ValueError("Attempt times must be finite numbers")


@dataclass(frozen=True)
class JobWatch:
    id: str
    server_alias: str
    job_id: str
    project_id: str
    thread_id: str
    session_id: str
    cwd: str
    last_state: str
    active: bool


@dataclass(frozen=True)
class TerminalEvent:
    id: str
    watch_id: str
    terminal_state: str
    delivery_state: str
    attempt_count: int
    lease_owner: str | None
    lease_until: float | None
    next_retry_at: float
    delivered_at: float | None
    created_at: float
    last_error: str | None
    send_started_at: float | None
    handoff_state: str
    turn_id: str | None
    handoff_finished_at: float | None
    handoff_error: str | None


@dataclass(frozen=True)
class ServerState:
    server_alias: str
    config_version: str
    failures: int
    next_retry_at: float
    paused: bool
    last_error: str | None


class AgentStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as db:
            version = db.execute("PRAGMA user_version").fetchone()[0]
            if version > 3:
                raise HpcMcpError("AGENT_DB_VERSION", "Agent database requires a newer version")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS job_watches (
                    id TEXT PRIMARY KEY, server_alias TEXT NOT NULL,
                    job_id TEXT NOT NULL, project_id TEXT NOT NULL,
                    thread_id TEXT NOT NULL, session_id TEXT NOT NULL,
                    cwd TEXT NOT NULL, last_state TEXT NOT NULL DEFAULT 'REGISTERED',
                    active INTEGER NOT NULL DEFAULT 1 CHECK(active IN (0,1)),
                    UNIQUE(server_alias, job_id));
                CREATE TABLE IF NOT EXISTS terminal_events (
                    id TEXT PRIMARY KEY, watch_id TEXT NOT NULL REFERENCES job_watches(id),
                    terminal_state TEXT NOT NULL, delivery_state TEXT NOT NULL DEFAULT 'PENDING',
                    attempt_count INTEGER NOT NULL DEFAULT 0, lease_owner TEXT,
                    lease_until REAL, next_retry_at REAL NOT NULL DEFAULT 0,
                    delivered_at REAL, created_at REAL NOT NULL,
                    UNIQUE(watch_id, terminal_state));
                CREATE TABLE IF NOT EXISTS server_states (
                    server_alias TEXT PRIMARY KEY, config_version TEXT NOT NULL,
                    failures INTEGER NOT NULL DEFAULT 0, next_retry_at REAL NOT NULL DEFAULT 0,
                    paused INTEGER NOT NULL DEFAULT 0, last_error TEXT);
            """)
            db.execute("BEGIN IMMEDIATE")
            # Additive v3 extension: rollback binaries ignore this diagnostic table.
            db.execute("""CREATE TABLE IF NOT EXISTS delivery_attempts (
                event_id TEXT NOT NULL REFERENCES terminal_events(id), attempt_number INTEGER NOT NULL,
                lease_owner TEXT NOT NULL, started_at REAL NOT NULL, updated_at REAL NOT NULL,
                finished_at REAL, stage TEXT NOT NULL, outcome TEXT NOT NULL,
                error_code TEXT, rpc_method TEXT, rpc_code INTEGER,
                cleanup_state TEXT NOT NULL DEFAULT 'NONE', cleanup_updated_at REAL, cleanup_error_code TEXT,
                PRIMARY KEY(event_id,attempt_number))""")
            columns = {row[1] for row in db.execute("PRAGMA table_info(terminal_events)")}
            if "last_error" not in columns:
                db.execute("ALTER TABLE terminal_events ADD COLUMN last_error TEXT")
            if "send_started_at" not in columns:
                db.execute("ALTER TABLE terminal_events ADD COLUMN send_started_at REAL")
            for name, definition in (("handoff_state", "TEXT NOT NULL DEFAULT 'NONE'"),
                    ("turn_id", "TEXT"), ("handoff_finished_at", "REAL"), ("handoff_error", "TEXT")):
                if name not in columns:
                    db.execute(f"ALTER TABLE terminal_events ADD COLUMN {name} {definition}")
            db.execute("PRAGMA user_version=3")
            db.execute("""CREATE TABLE IF NOT EXISTS queue_handoffs (
                event_id TEXT PRIMARY KEY REFERENCES terminal_events(id),
                format_version INTEGER NOT NULL DEFAULT 1,
                message_version INTEGER NOT NULL, message_sha256 TEXT NOT NULL,
                context_sha256 TEXT NOT NULL, state TEXT NOT NULL,
                queue_id TEXT, turn_id TEXT, intent_at REAL NOT NULL,
                acknowledged_at REAL, observed_at REAL, next_observe_at REAL NOT NULL,
                lease_owner TEXT, lease_until REAL, revision INTEGER NOT NULL DEFAULT 0,
                last_error TEXT)""")

            # Side table keeps rollback readers of queue_handoffs (SELECT *)
            # compatible; old completed rows and their send fences are untouched.
            db.execute("""CREATE TABLE IF NOT EXISTS queue_input_bindings (
                event_id TEXT PRIMARY KEY REFERENCES queue_handoffs(event_id),
                item_id TEXT NOT NULL, bound_turn_id TEXT NOT NULL,
                identity_kind TEXT NOT NULL, message_sha256 TEXT NOT NULL,
                context_sha256 TEXT NOT NULL, bound_at REAL NOT NULL,
                prior_identity_error TEXT)""")
            db.execute("""CREATE TABLE IF NOT EXISTS queue_reproof_origins (
                event_id TEXT PRIMARY KEY REFERENCES queue_handoffs(event_id),
                origin_error TEXT NOT NULL, created_at REAL NOT NULL)""")

    @contextmanager
    def _connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys=ON")
        try:
            with db:
                yield db
        finally:
            db.close()

    def register_watch(self, *, server_alias: str, job_id: str, project_id: str,
                       thread_id: str, session_id: str, cwd: str) -> JobWatch:
        if not re.fullmatch(r"[0-9]+(?:_[0-9]+)?", job_id):
            raise HpcMcpError("INVALID_JOB_ID", "Slurm Job ID is invalid")
        values = (server_alias, job_id, project_id, thread_id, session_id, cwd)
        if any(not value or not value.strip() or any(c in value for c in "\r\n\x00")
               for value in values):
            raise HpcMcpError("WATCH_CONTEXT_INVALID", "Watch context is invalid")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT * FROM job_watches WHERE server_alias=? AND job_id=?",
                             (server_alias, job_id)).fetchone()
            if row is not None:
                existing = JobWatch(**dict(row))
                if (existing.project_id, existing.thread_id, existing.session_id, existing.cwd) != values[2:]:
                    raise HpcMcpError("WATCH_ALREADY_REGISTERED", "Job watch is already bound to another context")
                return existing
            watch_id = str(uuid4())
            db.execute("""INSERT INTO job_watches
                (id,server_alias,job_id,project_id,thread_id,session_id,cwd)
                VALUES (?,?,?,?,?,?,?)""", (watch_id, *values))
            return JobWatch(watch_id, *values, "REGISTERED", True)

    def watches(self, *, active: bool = False) -> list[JobWatch]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM job_watches" + (" WHERE active=1" if active else "")
                              + " ORDER BY server_alias,job_id").fetchall()
        return [JobWatch(**dict(row)) for row in rows]

    def get_watch(self, watch_id: str) -> JobWatch | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM job_watches WHERE id=?", (watch_id,)).fetchone()
        return JobWatch(**dict(row)) if row else None

    def find_watch(self, server_alias: str, job_id: str) -> JobWatch | None:
        with self._connect() as db:
            row = db.execute("SELECT * FROM job_watches WHERE server_alias=? AND job_id=?",
                             (server_alias, job_id)).fetchone()
        return JobWatch(**dict(row)) if row else None

    def set_state(self, watch_id: str, state: str) -> None:
        with self._connect() as db:
            db.execute("UPDATE job_watches SET last_state=? WHERE id=? AND active=1", (state, watch_id))

    def record_terminal(self, watch_id: str, state: str, *, now: float) -> TerminalEvent:
        if state not in TERMINAL_STATES:
            raise HpcMcpError("INVALID_TERMINAL_STATE", "Accounting state is not terminal")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            db.execute("""INSERT OR IGNORE INTO terminal_events
                (id,watch_id,terminal_state,created_at) VALUES (?,?,?,?)""",
                (str(uuid4()), watch_id, state, now))
            db.execute("UPDATE job_watches SET last_state=?,active=0 WHERE id=?", (state, watch_id))
            row = db.execute("SELECT * FROM terminal_events WHERE watch_id=? AND terminal_state=?",
                             (watch_id, state)).fetchone()
        return TerminalEvent(**dict(row))

    def events(self) -> list[TerminalEvent]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM terminal_events ORDER BY created_at,id").fetchall()
        return [TerminalEvent(**dict(row)) for row in rows]

    def claim_event(self, owner: str, *, now: float, lease_seconds: float = 120) -> TerminalEvent | None:
        _finite_times(now, lease_seconds, now + lease_seconds)
        if not owner or lease_seconds <= 0:
            raise ValueError("A positive lease and owner are required")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("""SELECT * FROM terminal_events
                WHERE delivery_state IN ('PENDING','IN_FLIGHT') AND next_retry_at<=?
                AND NOT EXISTS (SELECT 1 FROM queue_handoffs q WHERE q.event_id=terminal_events.id)
                AND (lease_until IS NULL OR lease_until<=?) ORDER BY created_at,id LIMIT 1""",
                (now, now)).fetchone()
            if row is None:
                return None
            db.execute("""UPDATE delivery_attempts SET outcome='ABANDONED',
                error_code='CODEX_ATTEMPT_ABANDONED',updated_at=?,finished_at=?
                WHERE event_id=? AND attempt_number=? AND outcome='RUNNING'""",
                (now, now, row["id"], row["attempt_count"]))
            db.execute("""UPDATE terminal_events SET delivery_state='IN_FLIGHT',
                lease_owner=?,lease_until=?,attempt_count=attempt_count+1 WHERE id=?""",
                (owner, now + lease_seconds, row["id"]))
            event = db.execute("SELECT * FROM terminal_events WHERE id=?", (row["id"],)).fetchone()
            db.execute("""INSERT INTO delivery_attempts
                (event_id,attempt_number,lease_owner,started_at,updated_at,stage,outcome)
                VALUES (?,?,?,?,?,'CLAIM','RUNNING')""",
                (event["id"], event["attempt_count"], owner, now, now))
            db.execute("""DELETE FROM delivery_attempts WHERE event_id=? AND attempt_number NOT IN
                (SELECT attempt_number FROM delivery_attempts WHERE event_id=?
                 ORDER BY attempt_number DESC LIMIT 200)""", (event["id"], event["id"]))
        return TerminalEvent(**dict(event))

    def record_attempt_stage(self, event_id: str, owner: str, stage: str, *, now: float,
                             rpc_method=None, rpc_code=None) -> bool:
        _finite_times(now)
        stage = stage if isinstance(stage, str) and stage in ATTEMPT_STAGES else "UNKNOWN"
        method, code = safe_rpc(rpc_method, rpc_code)
        with self._connect() as db:
            return db.execute("""UPDATE delivery_attempts SET stage=?,updated_at=?,rpc_method=?,rpc_code=?
                WHERE event_id=? AND lease_owner=? AND outcome='RUNNING' AND attempt_number IN
                (SELECT attempt_count FROM terminal_events WHERE id=? AND lease_owner=?
                 AND lease_until>? AND delivery_state='IN_FLIGHT')""",
                (stage, now, method, code, event_id, owner, event_id, owner, now)).rowcount == 1

    def record_attempt_cleanup(self, event_id: str, owner: str, attempt_number: int,
                               state: str, *, now: float) -> bool:
        """Keep teardown evidence separate from the causal delivery stage.

        Accepted and manual-recovery attempts have no live event lease; their
        exact journal owner and attempt number still fence cleanup, and can never
        authorize another send.
        """
        _finite_times(now)
        if state not in {"RUNNING", "CONFIRMED", "UNCONFIRMED"}:
            raise ValueError("Invalid cleanup outcome")
        with self._connect() as db:
            return db.execute("""UPDATE delivery_attempts SET cleanup_state=?,cleanup_updated_at=?,cleanup_error_code=?
                WHERE event_id=? AND lease_owner=? AND attempt_number=? AND EXISTS
                (SELECT 1 FROM terminal_events WHERE id=? AND attempt_count=? AND
                 ((delivery_state='IN_FLIGHT' AND lease_owner=? AND lease_until>?)
                  OR delivery_state IN ('DELIVERED','MANUAL_REQUIRED','QUEUED')
                  OR EXISTS (SELECT 1 FROM queue_handoffs q WHERE q.event_id=terminal_events.id)))""",
                (state, now, "CODEX_CHILD_CLEANUP_UNCONFIRMED" if state == "UNCONFIRMED" else None,
                 event_id, owner, attempt_number, event_id, attempt_number, owner, now)).rowcount == 1

    def _finish_attempt(self, db, event_id, owner, now, outcome, error_code=None,
                        rpc_method=None, rpc_code=None):
        if outcome not in ATTEMPT_OUTCOMES:
            raise ValueError("Invalid attempt outcome")
        method, code = safe_rpc(rpc_method, rpc_code)
        db.execute("""UPDATE delivery_attempts SET outcome=?,updated_at=?,finished_at=?,
            error_code=?,rpc_method=?,rpc_code=?,stage=CASE WHEN ?='DELIVERED' THEN 'ACCEPTED' ELSE stage END
            WHERE event_id=? AND lease_owner=? AND outcome='RUNNING'
            AND attempt_number=(SELECT attempt_count FROM terminal_events WHERE id=?)""",
            (outcome, now, now, safe_delivery_code(error_code) if error_code is not None else None,
             method, code, outcome, event_id, owner, event_id))

    def diagnostics(self, event_id: str, *, limit: int = 20) -> dict:
        if type(limit) is not int or not 1 <= limit <= 100:
            raise ValueError("Diagnostic limit must be between 1 and 100")
        with self._connect() as db:
            db.execute("BEGIN")
            event = db.execute("SELECT attempt_count FROM terminal_events WHERE id=?", (event_id,)).fetchone()
            if event is None:
                raise HpcMcpError("EVENT_NOT_FOUND", "Completion event was not found")
            count = db.execute("SELECT COUNT(*) FROM delivery_attempts WHERE event_id=?", (event_id,)).fetchone()[0]
            rows = db.execute("""SELECT attempt_number,started_at,updated_at,finished_at,stage,outcome,
                error_code,rpc_method,rpc_code,cleanup_state,cleanup_updated_at,cleanup_error_code
                FROM delivery_attempts WHERE event_id=?
                ORDER BY attempt_number DESC LIMIT ?""", (event_id, limit)).fetchall()
        return {"event_id": event_id, "total_attempt_count": event[0], "retained_count": count,
                "returned_count": len(rows), "gap_count": event[0] - count,
                "attempts": [dict(row) for row in rows]}

    def mark_delivered(self, event_id: str, owner: str, *, now: float,
                       turn_id: str | None = None, handoff_state: str = "UNKNOWN") -> bool:
        _finite_times(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE terminal_events SET delivery_state='DELIVERED',
                delivered_at=?,lease_owner=NULL,lease_until=NULL,last_error=NULL,
                turn_id=?,handoff_state=?,handoff_error=?,handoff_finished_at=?
                WHERE id=? AND lease_owner=? AND lease_until>? AND delivery_state='IN_FLIGHT'""",
                (now, turn_id, handoff_state,
                 "CODEX_HISTORY_ACCEPTED_OUTCOME_UNKNOWN" if handoff_state == "UNKNOWN" else None,
                 now if handoff_state in {"COMPLETED", "FAILED", "INTERRUPTED"} else None,
                 event_id, owner, now)).rowcount == 1
            if changed:
                self._finish_attempt(db, event_id, owner, now, "DELIVERED")
            return changed

    def finish_handoff(self, event_id: str, turn_id: str, state: str, *, now: float,
                       error_code: str | None = None) -> bool:
        if state not in {"COMPLETED", "FAILED", "INTERRUPTED", "UNKNOWN"}:
            raise ValueError("Invalid handoff outcome")
        with self._connect() as db:
            return db.execute("""UPDATE terminal_events SET handoff_state=?,
                handoff_finished_at=?,handoff_error=? WHERE id=? AND turn_id=?
                AND handoff_state='RUNNING' AND delivery_state='DELIVERED'""",
                (state, now, safe_delivery_code(error_code) if error_code is not None else None,
                 event_id, turn_id)).rowcount == 1

    def require_manual_recovery(self, event_id: str, owner: str, *, now: float, code: str,
                                rpc_method=None, rpc_code=None) -> bool:
        _finite_times(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE terminal_events SET delivery_state='MANUAL_REQUIRED',
                handoff_state='UNKNOWN',handoff_error=?,lease_owner=NULL,lease_until=NULL
                WHERE id=? AND lease_owner=? AND lease_until>? AND delivery_state='IN_FLIGHT'""",
                (safe_delivery_code(code), event_id, owner, now)).rowcount == 1
            if changed:
                self._finish_attempt(db, event_id, owner, now, "MANUAL_REQUIRED", code, rpc_method, rpc_code)
            return changed

    def record_cleanup_failure(self, event_id: str) -> None:
        """Preserve the accepted receipt and outcome; expose an unresolved child."""
        with self._connect() as db:
            db.execute("""UPDATE terminal_events
                SET handoff_error='CODEX_CHILD_CLEANUP_UNCONFIRMED'
                WHERE id=? AND (delivery_state='DELIVERED' OR EXISTS
                (SELECT 1 FROM queue_handoffs q WHERE q.event_id=terminal_events.id))""", (event_id,))
            db.execute("""UPDATE queue_handoffs SET last_error=COALESCE(last_error,
                'CODEX_CHILD_CLEANUP_UNCONFIRMED') WHERE event_id=?""", (event_id,))

    def recover_interrupted_handoffs(self, *, now: float) -> None:
        """Call once at single-instance agent startup, never from status/readers."""
        with self._connect() as db:
            db.execute("""UPDATE terminal_events SET handoff_state='UNKNOWN',
                handoff_finished_at=?,handoff_error='CODEX_AGENT_RESTARTED'
                WHERE delivery_state='DELIVERED' AND handoff_state IN ('NONE','RUNNING')
                AND NOT EXISTS (SELECT 1 FROM queue_handoffs q WHERE q.event_id=terminal_events.id)""", (now,))

    def mark_send_started(self, event_id: str, owner: str, *, now: float) -> bool:
        """Persist intent and fence an expired/stolen worker before turn/start."""
        _finite_times(now)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE terminal_events SET send_started_at=COALESCE(send_started_at,?)
                WHERE id=? AND lease_owner=? AND lease_until>? AND delivery_state='IN_FLIGHT'""",
                (now, event_id, owner, now)).rowcount == 1
            if changed:
                db.execute("""UPDATE delivery_attempts SET stage='SEND_ACK',updated_at=?
                    WHERE event_id=? AND lease_owner=? AND outcome='RUNNING'""", (now, event_id, owner))
            return changed

    def defer_event(self, event_id: str, owner: str, *, retry_at: float, now: float,
                    error_code: str | None = None, rpc_method=None, rpc_code=None,
                    cancelled: bool = False) -> bool:
        _finite_times(now, retry_at)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            changed = db.execute("""UPDATE terminal_events SET delivery_state='PENDING',
                next_retry_at=?,lease_owner=NULL,lease_until=NULL,last_error=?
                WHERE id=? AND lease_owner=? AND lease_until>? AND delivery_state='IN_FLIGHT'""",
                (retry_at, safe_delivery_code(error_code) if error_code is not None else None,
                 event_id, owner, now)).rowcount == 1
            if changed:
                self._finish_attempt(db, event_id, owner, now, "CANCELLED" if cancelled else "DEFERRED",
                                     error_code, rpc_method, rpc_code)
            return changed

    def server_states(self) -> list[ServerState]:
        with self._connect() as db:
            rows = db.execute("SELECT * FROM server_states ORDER BY server_alias").fetchall()
        return [ServerState(**dict(row)) for row in rows]

    def server_ready(self, alias: str, version: str, *, now: float) -> bool:
        with self._connect() as db:
            db.execute("""INSERT INTO server_states(server_alias,config_version) VALUES (?,?)
                ON CONFLICT(server_alias) DO UPDATE SET config_version=excluded.config_version,
                failures=0,next_retry_at=0,paused=0,last_error=NULL
                WHERE config_version != excluded.config_version""", (alias, version))
            row = db.execute("SELECT * FROM server_states WHERE server_alias=?", (alias,)).fetchone()
        return not row["paused"] and row["next_retry_at"] <= now

    def server_error(self, alias: str, code: str, *, now: float, pause: bool = False) -> None:
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT failures FROM server_states WHERE server_alias=?", (alias,)).fetchone()
            failures = min(row[0] + 1, 10)
            delay = min(60 * 2 ** (failures - 1), 600)
            db.execute("""UPDATE server_states SET failures=?,next_retry_at=?,paused=?,last_error=?
                WHERE server_alias=?""", (failures, now + delay, int(pause), code, alias))

    def retry_server(self, alias: str) -> None:
        with self._connect() as db:
            db.execute("""UPDATE server_states SET failures=0,next_retry_at=0,paused=0,last_error=NULL
                WHERE server_alias=?""", (alias,))
