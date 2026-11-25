import pytest
import json
from backend.database.connection import OrchestratorDB

@pytest.fixture
def db():
    instance = OrchestratorDB(":memory:")
    yield instance
    instance.close()

def _make_stage(db, bp_id, name="Calculate", stype="calculation"):
    return db.conn.execute(
        "INSERT INTO stage (business_process_id, name, stage_type) VALUES (?, ?, ?)",
        (bp_id, name, stype),
    ).lastrowid

def _make_bp_request(db, bp_id, date="2026-11-24", initiator="user1", payload=None):
    return db.create_bp_request(bp_id, date, "manual", initiator, payload or {})

def test_schema_creation(db):
    tables = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [t['name'] for t in tables]
    assert 'bp_request' in table_names
    assert 'stage_request' in table_names
    assert 'run' in table_names
    assert 'lock' in table_names
    assert 'event' in table_names

def test_deduplication(db):
    bp_id = db.create_business_process("Test", "credit", [])
    res1 = _make_bp_request(db, bp_id, payload={"a": 1})
    assert res1["status"] == "created"
    res2 = _make_bp_request(db, bp_id, payload={"a": 1})
    assert res2["status"] == "merged"
    assert res2["id"] == res1["id"]

def test_business_process_crud(db):
    bp_id = db.create_business_process("Credit Risk", "credit", {})
    bp = db.get_business_process(bp_id)
    assert bp["name"] == "Credit Risk"
    assert bp["risk_type"] == "credit"

    all_bps = db.get_all_business_processes()
    assert len(all_bps) == 1

    db.update_business_process(bp_id, "Updated Credit", "market", {"v": 2})
    bp = db.get_business_process(bp_id)
    assert bp["name"] == "Updated Credit"
    assert bp["risk_type"] == "market"

def test_stage_crud(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    stage = db.get_stage(stage_id)
    assert stage["name"] == "Calculate"
    assert stage["business_process_id"] == bp_id

    stages = db.get_stages(bp_id)
    assert len(stages) == 1

def test_create_stage_request(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)

    sr1 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    row = db.conn.execute("SELECT status, parent_stage_request_ids FROM stage_request WHERE stage_request_id = ?", (sr1,)).fetchone()
    assert row["status"] == "created"
    assert row["parent_stage_request_ids"] is None

    sr2 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash2", [sr1])
    row2 = db.conn.execute("SELECT status, parent_stage_request_ids FROM stage_request WHERE stage_request_id = ?", (sr2,)).fetchone()
    assert row2["status"] == "waiting_dependency"
    assert json.loads(row2["parent_stage_request_ids"]) == [sr1]

def test_get_stage_requests_by_bp_request(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    db.create_stage_request(bp_req["id"], stage_id, "2026-11-25", 5, "hash2", None)

    requests = db.get_stage_requests_by_bp_request(bp_req["id"])
    assert len(requests) == 2

def test_update_stage_request_status(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)

    db.update_stage_request_status(sr, "queued", {"type": "resource", "id": "deals/2026-11-24"})
    row = db.conn.execute("SELECT status, waiting_for FROM stage_request WHERE stage_request_id = ?", (sr,)).fetchone()
    assert row["status"] == "queued"
    assert json.loads(row["waiting_for"]) == {"type": "resource", "id": "deals/2026-11-24"}

def test_get_stage_requests_by_parent(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr1 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    sr2 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash2", [sr1])
    sr3 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash3", [sr1])

    children = db.get_stage_requests_by_parent(sr1)
    assert len(children) == 2
    assert {c["stage_request_id"] for c in children} == {sr2, sr3}

def test_create_run(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)

    run_id = db.create_run(sr)
    run = db.get_run_by_stage_request(sr)
    assert run["run_id"] == run_id
    assert run["status"] == "queued"

def test_update_run_status(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)

    db.update_run_status(run_id, "running", started_at="2026-11-24T10:00:00")
    run = db.get_run_by_stage_request(sr)
    assert run["status"] == "running"
    assert run["started_at"] == "2026-11-24T10:00:00"

def test_get_runs_by_status(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)
    db.update_run_status(run_id, "running")

    running = db.get_runs_by_status("running")
    assert len(running) == 1
    assert running[0]["run_id"] == run_id

def test_acquire_and_release_locks(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)

    locks = [
        {"resource_id": "deals", "granularity": "table", "data_key": "2026-11-24", "mode": "write"},
        {"resource_id": "rates", "granularity": "table", "data_key": "2026-11-24", "mode": "read"},
    ]
    assert db.acquire_locks(run_id, locks) is True

    active = db.get_active_locks("deals", "2026-11-24")
    assert len(active) == 1
    assert active[0]["mode"] == "write"

    locks_by_run = db.get_locks_by_run(run_id)
    assert len(locks_by_run) == 2

    released = db.release_locks(run_id)
    assert released == 2

    active_after = db.get_active_locks("deals", "2026-11-24")
    assert len(active_after) == 0

def test_acquire_locks_conflict(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr1 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    sr2 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash2", None)
    run1 = db.create_run(sr1)
    run2 = db.create_run(sr2)

    assert db.acquire_locks(run1, [{"resource_id": "deals", "granularity": "table", "data_key": "2026-11-24", "mode": "write"}]) is True
    assert db.acquire_locks(run2, [{"resource_id": "deals", "granularity": "table", "data_key": "2026-11-24", "mode": "write"}]) is False

def test_finish_run(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)

    db.finish_run(run_id, "done")
    run = db.get_run_by_stage_request(sr)
    assert run["status"] == "done"
    assert run["finished_at"] is not None

def test_wake_children(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr_parent = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    sr_child = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash2", [sr_parent])

    run_parent = db.create_run(sr_parent)
    db.finish_run(run_parent, "done")

    woken = db.wake_children(sr_parent)
    assert sr_child in woken

    child = db.conn.execute("SELECT status FROM stage_request WHERE stage_request_id = ?", (sr_child,)).fetchone()
    assert child["status"] == "queued"

def test_check_dependencies(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr_parent = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    sr_child = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash2", [sr_parent])

    ok, unfinished = db.check_dependencies(sr_child)
    assert ok is False
    assert sr_parent in unfinished

    run_parent = db.create_run(sr_parent)
    db.finish_run(run_parent, "done")
    ok, unfinished = db.check_dependencies(sr_child)
    assert ok is True
    assert unfinished == []

def test_get_all_queued_requests(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr1 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 5, "hash2", None)
    sr3 = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 1, "hash3", [sr1])

    queued = db.get_all_queued_requests()
    statuses = {q["status"] for q in queued}
    assert "created" in statuses
    assert "waiting_dependency" in statuses

def test_expired_locks(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)

    db.acquire_locks(run_id, [{"resource_id": "deals", "granularity": "table", "data_key": "2026-11-24", "mode": "write"}])
    db.conn.execute("UPDATE lock SET expires_at = datetime('now', '-1 minute') WHERE run_id = ?", (run_id,))

    expired = db.get_expired_locks()
    assert len(expired) == 1

    count = db.expire_locks()
    assert count == 1

def test_update_heartbeat_and_stale_runs(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)
    db.update_run_status(run_id, "running")

    assert db.update_heartbeat(run_id) is True
    run = db.get_run_by_stage_request(sr)
    assert run["heartbeat_at"] is not None
    assert run["lease_expires_at"] is not None

    stale = db.get_stale_runs(30)
    assert len(stale) == 0

    db.conn.execute("UPDATE run SET lease_expires_at = datetime('now', '-1 minute') WHERE run_id = ?", (run_id,))
    stale = db.get_stale_runs(30)
    assert len(stale) == 1

def test_events(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)

    db.log_event("stage_created", "stage_request", sr, {"stage_id": stage_id}, "system")
    db.log_event("run_started", "run", 1, {"run_id": 1}, "system")

    events = db.get_events_by_entity("stage_request", sr)
    assert len(events) == 1
    assert events[0]["event_type"] == "stage_created"

    recent = db.get_recent_events(5)
    assert len(recent) == 2

    by_type = db.get_events_by_type("stage_created")
    assert len(by_type) == 1

def test_external_events(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)
    db.update_run_status(run_id, "waiting_external")

    db.update_run_external_event(run_id, {"actor": "reviewer", "decision": "approve"})
    runs = db.get_runs_waiting_external()
    assert len(runs) == 1
    assert runs[0]["run_id"] == run_id
    assert json.loads(runs[0]["external_event"])["decision"] == "approve"

def test_queue_snapshot(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)

    snapshot = db.get_queue_snapshot()
    assert len(snapshot) == 1
    assert snapshot[0]["stage_request_id"] == sr

def test_locks_snapshot(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)
    db.acquire_locks(run_id, [{"resource_id": "deals", "granularity": "table", "data_key": "2026-11-24", "mode": "write"}])

    snapshot = db.get_locks_snapshot()
    assert len(snapshot) == 1
    assert snapshot[0]["resource_id"] == "deals"

def test_get_bp_request_full(db):
    bp_id = db.create_business_process("Test", "credit", [])
    stage_id = _make_stage(db, bp_id)
    bp_req = _make_bp_request(db, bp_id)
    sr = db.create_stage_request(bp_req["id"], stage_id, "2026-11-24", 10, "hash1", None)
    run_id = db.create_run(sr)

    full = db.get_bp_request_full(bp_req["id"])
    assert len(full) == 1
    assert full[0]["bp_request_id"] == bp_req["id"]
    assert full[0]["stage_request_id"] == sr
    assert full[0]["run_id"] == run_id
