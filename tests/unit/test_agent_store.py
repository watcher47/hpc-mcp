from concurrent.futures import ThreadPoolExecutor

import pytest

from hpc_mcp.errors import HpcMcpError


def make_watch(store, job_id="123", **changes):
    values = dict(server_alias="cluster", job_id=job_id, project_id="project",
                  thread_id="thread", session_id="session", cwd="/project")
    return store.register_watch(**(values | changes))


def test_watch_survives_restart_and_cannot_be_reassigned(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    path = tmp_path / "agent.sqlite3"
    store = AgentStore(path)
    watch = make_watch(store, "123_7")
    assert make_watch(AgentStore(path), "123_7").id == watch.id
    assert AgentStore(path).watches()[0] == watch
    with pytest.raises(HpcMcpError, match="already"):
        make_watch(store, "123_7", thread_id="other")
    with pytest.raises(HpcMcpError):
        make_watch(store, "123;bad")


def test_terminal_event_and_deactivation_are_atomic_idempotent(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "agent.sqlite3")
    watch = make_watch(store)
    store.set_state(watch.id, "VERIFYING")
    assert store.watches(active=True)[0].last_state == "VERIFYING"
    event = store.record_terminal(watch.id, "COMPLETED", now=10)
    assert store.record_terminal(watch.id, "COMPLETED", now=11).id == event.id
    assert store.watches(active=True) == []
    assert len(store.events()) == 1
    assert store.events()[0].delivery_state == "PENDING"
    with pytest.raises(HpcMcpError):
        store.record_terminal(watch.id, "RUNNING", now=12)


def test_atomic_lease_expiry_retry_and_delivery(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    path = tmp_path / "agent.sqlite3"
    store = AgentStore(path)
    event = store.record_terminal(make_watch(store).id, "FAILED", now=10)
    def claim(owner):
        return AgentStore(path).claim_event(owner, now=11, lease_seconds=20)
    with ThreadPoolExecutor(2) as pool:
        claims = list(pool.map(claim, ("worker-a", "worker-b")))
    claimed = [c for c in claims if c is not None]
    assert len(claimed) == 1
    assert claimed[0].attempt_count == 1
    assert store.claim_event("worker-c", now=30) is None
    recovered = AgentStore(path).claim_event("worker-c", now=32)
    assert recovered.id == event.id
    assert recovered.attempt_count == 2
    assert not store.mark_delivered(event.id, "wrong-owner", now=33)
    assert store.defer_event(event.id, "worker-c", retry_at=70, now=34)
    assert store.claim_event("worker-d", now=69) is None
    assert store.claim_event("worker-d", now=70).id == event.id
    assert store.mark_delivered(event.id, "worker-d", now=71)
    assert store.events()[0].delivered_at == 71
    assert store.claim_event("worker-e", now=1000) is None


def test_expired_worker_cannot_defer_lease(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "FAILED", now=0)
    store.claim_event("old", now=0, lease_seconds=10)
    assert not store.defer_event(event.id, "old", retry_at=1000, now=11)
    assert store.claim_event("new", now=11).id == event.id


def test_attempt_journal_survives_reopen_and_delivery(tmp_path):
    from hpc_mcp.agent.store import AgentStore, ATTEMPT_OUTCOMES
    path = tmp_path / "db"
    store = AgentStore(path)
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    store.claim_event("first", now=1)
    assert store.record_attempt_stage(event.id, "first", "SCHEMA", now=2)
    assert store.defer_event(event.id, "first", retry_at=4, now=3,
                             error_code="CODEX_PROTOCOL_INCOMPATIBLE")
    store = AgentStore(path)
    store.claim_event("second", now=4)
    assert store.mark_delivered(event.id, "second", now=5)
    report = store.diagnostics(event.id)
    assert report["total_attempt_count"] == report["retained_count"] == 2
    assert report["gap_count"] == 0
    assert [(a["attempt_number"], a["outcome"], a["error_code"]) for a in report["attempts"]] == [
        (2, "DELIVERED", None), (1, "DEFERRED", "CODEX_PROTOCOL_INCOMPATIBLE")]
    assert report["attempts"][1]["stage"] == "SCHEMA"
    assert store.events()[0].last_error is None
    assert all(row["outcome"] in ATTEMPT_OUTCOMES for row in report["attempts"])


def test_journal_fences_reclaimed_attempt_and_reports_legacy_gap(tmp_path):
    import sqlite3
    from hpc_mcp.agent.store import AgentStore
    path = tmp_path / "db"
    store = AgentStore(path)
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    with sqlite3.connect(path) as db:
        db.execute("UPDATE terminal_events SET attempt_count=105")
    before = store.events()[0]
    store = AgentStore(path)
    assert store.events()[0] == before
    assert store.diagnostics(event.id)["gap_count"] == 105
    assert store.diagnostics(event.id)["attempts"] == []
    store.claim_event("old", now=1, lease_seconds=2)
    store.claim_event("new", now=4)
    assert not store.record_attempt_stage(event.id, "old", "SEND_ACK", now=4)
    assert not store.defer_event(event.id, "old", retry_at=10, now=4)
    rows = store.diagnostics(event.id)["attempts"]
    assert rows[1]["outcome"] == "ABANDONED"
    assert rows[1]["finished_at"] == 4
    assert rows[0]["stage"] == "CLAIM"
    with sqlite3.connect(path) as db:
        assert db.execute("PRAGMA user_version").fetchone()[0] == 3


def test_journal_bounds_retention_and_rejects_hostile_fields(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    secret = "CODEX_PRIVATE_SECRET_" + "A" * 10000
    for i in range(205):
        store.claim_event("owner", now=i * 2)
        store.record_attempt_stage(event.id, "owner", secret, now=i * 2,
            rpc_method=secret, rpc_code=True)
        store.defer_event(event.id, "owner", now=i * 2 + 1, retry_at=i * 2 + 1,
            error_code=secret, rpc_method=secret, rpc_code=2**40)
    report = store.diagnostics(event.id, limit=100)
    assert (report["total_attempt_count"], report["retained_count"],
            report["returned_count"], report["gap_count"]) == (205, 200, 100, 5)
    assert report["attempts"][0]["error_code"] == "CODEX_HANDOFF_FAILED"
    assert report["attempts"][0]["stage"] == "UNKNOWN"
    assert report["attempts"][0]["rpc_method"] is None
    assert report["attempts"][0]["rpc_code"] is None
    assert secret.encode() not in store.path.read_bytes()
    for limit in (0, 101, True):
        with pytest.raises(ValueError):
            store.diagnostics(event.id, limit=limit)


def test_cleanup_evidence_fenced_to_exact_accepted_attempt(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    store.claim_event("old", now=1, lease_seconds=2)
    store.claim_event("new", now=4)
    assert store.mark_delivered(event.id, "new", now=5)
    assert not store.record_attempt_cleanup(event.id, "old", 1, "RUNNING", now=6)
    assert store.record_attempt_cleanup(event.id, "new", 2, "RUNNING", now=6)
    assert store.record_attempt_cleanup(event.id, "new", 2, "CONFIRMED", now=7)
    row = store.diagnostics(event.id)["attempts"][0]
    assert (row["stage"], row["outcome"], row["cleanup_state"], row["cleanup_updated_at"]) == (
        "ACCEPTED", "DELIVERED", "CONFIRMED", 7)
    assert store.events()[0].delivery_state == "DELIVERED"


def test_manual_cleanup_evidence_fenced_to_original_exact_attempt(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    store.claim_event("old", now=1, lease_seconds=2)
    store.claim_event("new", now=4)
    assert store.require_manual_recovery(event.id, "new", now=5,
        code="CODEX_SEND_OUTCOME_UNKNOWN")
    before = store.events()[0]
    assert not store.record_attempt_cleanup(event.id, "old", 1, "CONFIRMED", now=6)
    assert not store.record_attempt_cleanup(event.id, "other", 2, "CONFIRMED", now=6)
    assert not store.record_attempt_cleanup(event.id, "new", 1, "CONFIRMED", now=6)
    assert store.record_attempt_cleanup(event.id, "new", 2, "RUNNING", now=6)
    assert store.record_attempt_cleanup(event.id, "new", 2, "UNCONFIRMED", now=7)
    row = store.diagnostics(event.id)["attempts"][0]
    assert row["error_code"] == "CODEX_SEND_OUTCOME_UNKNOWN"
    assert row["cleanup_state"] == "UNCONFIRMED"
    assert row["cleanup_error_code"] == "CODEX_CHILD_CLEANUP_UNCONFIRMED"
    assert store.events()[0] == before
    with pytest.raises(ValueError):
        store.record_attempt_cleanup(event.id, "new", 2, "unexpected", now=8)


def test_nonfinite_attempt_times_rejected_without_state_change(tmp_path):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    for now in (float("inf"), float("nan"), True):
        with pytest.raises(ValueError):
            store.claim_event("owner", now=now)
    assert store.events()[0].attempt_count == 0
    store.claim_event("owner", now=1)
    for call in (
        lambda: store.record_attempt_stage(event.id, "owner", "SCHEMA", now=float("inf")),
        lambda: store.defer_event(event.id, "owner", retry_at=float("inf"), now=2),
        lambda: store.mark_delivered(event.id, "owner", now=float("inf")),
    ):
        with pytest.raises(ValueError):
            call()
    assert store.diagnostics(event.id)["attempts"][0]["stage"] == "CLAIM"


def test_additive_legacy_migration_preserves_completed_receipt(tmp_path):
    import sqlite3
    from hpc_mcp.agent.store import AgentStore
    path = tmp_path / "legacy"
    store = AgentStore(path)
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    with sqlite3.connect(path) as db:
        db.execute("DROP TABLE delivery_attempts")
        db.execute("""UPDATE terminal_events SET attempt_count=105,delivery_state='DELIVERED',
            delivered_at=10,send_started_at=9,handoff_state='COMPLETED',turn_id='accepted-turn',
            handoff_finished_at=11""")
    before = store.events()[0]
    reopened = AgentStore(path)
    assert reopened.events()[0] == before
    assert reopened.diagnostics(event.id)["retained_count"] == 0
    assert reopened.diagnostics(event.id)["gap_count"] == 105
    assert reopened.claim_event("new", now=1000) is None


def test_attempt_finish_failure_rolls_back_delivery_transition(tmp_path):
    import sqlite3
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / "db")
    event = store.record_terminal(make_watch(store).id, "COMPLETED", now=0)
    store.claim_event("owner", now=1)
    with sqlite3.connect(store.path) as db:
        db.execute("""CREATE TRIGGER reject_finish BEFORE UPDATE OF outcome ON delivery_attempts
            BEGIN SELECT RAISE(ABORT, 'synthetic write failure'); END""")
    with pytest.raises(sqlite3.IntegrityError):
        store.mark_delivered(event.id, "owner", now=2)
    assert store.events()[0].delivery_state == "IN_FLIGHT"
    assert store.diagnostics(event.id)["attempts"][0]["outcome"] == "RUNNING"


@pytest.mark.parametrize('outcome', ['COMPLETED', 'FAILED', 'INTERRUPTED', 'RUNNING', 'UNKNOWN'])
def test_verified_delivery_records_only_terminal_observation_time(tmp_path, outcome):
    from hpc_mcp.agent.store import AgentStore
    store = AgentStore(tmp_path / 'db')
    event = store.record_terminal(make_watch(store).id, 'COMPLETED', now=1)
    store.claim_event('owner', now=2)
    assert not store.mark_delivered(event.id, 'wrong-owner', now=3,
        turn_id='exact-turn', handoff_state=outcome)
    assert store.events()[0].handoff_finished_at is None
    assert store.mark_delivered(event.id, 'owner', now=4,
        turn_id='exact-turn', handoff_state=outcome)
    before = store.events()
    assert before[0].handoff_finished_at == (4 if outcome in {'COMPLETED', 'FAILED', 'INTERRUPTED'} else None)
    assert not store.mark_delivered(event.id, 'owner', now=5,
        turn_id='exact-turn', handoff_state='COMPLETED')
    reopened = AgentStore(store.path)
    assert reopened.events() == before
