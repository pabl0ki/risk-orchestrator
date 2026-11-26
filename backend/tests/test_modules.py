import json

import pytest

from backend.orchestrator.admit import Admit
from backend.orchestrator.engine import Engine
from backend.orchestrator.expand import Expand


@pytest.fixture
def engine(tmp_path):
    return Engine(tmp_path / "orchestrator.sqlite3")


@pytest.mark.parametrize("parents,reason", [("self", "circular_dependency"), ([99999], "invalid_route")])
def test_invalid_route_commits_rejection_without_stages(engine, parents, reason):
    with engine.tx() as db:
        pid = db.create_business_process("Test DAG", "test", [])
        sid = db.create_stage(pid, "calculation", "[]", None, 1, 10)
        dependencies = [sid] if parents == "self" else parents
        db.conn.execute("UPDATE stage SET depends_on_stages=? WHERE stage_id=?", (json.dumps(dependencies), sid))
        bid = db.create_bp_request(pid, "2026-11-26", "manual", "Analyst", {})["id"]
        assert Expand(db).expand_bp_request(bid)["reason"] == reason
    with engine.tx() as db:
        assert db.get_stage_requests_by_bp_request(bid) == []
        assert db.conn.execute("SELECT status FROM bp_request WHERE bp_request_id=?", (bid,)).fetchone()[0] == "rejected"


def test_failed_lock_acquisition_rolls_back_run(engine, monkeypatch):
    bid = engine.submit(dict(kind="P1", day="2026-11-26", actor="Analyst", approval=True))
    with engine.tx() as db:
        sid = db.get_stage_requests_by_bp_request(bid)[0]["stage_request_id"]
        with monkeypatch.context() as patch:
            acquire = db.acquire_locks
            def interrupted(run_id, locks):
                assert acquire(run_id, locks)
                return False
            patch.setattr(db, "acquire_locks", interrupted)
            with pytest.raises(RuntimeError):
                Admit(db).admit_stage_request(sid)
        assert db.conn.execute("SELECT count(*) FROM run").fetchone()[0] == 0
        assert db.conn.execute("SELECT count(*) FROM lock").fetchone()[0] == 0
        assert Admit(db).admit_stage_request(sid)["decision"] == "running"


def test_stale_run_released_once(engine):
    bid = engine.submit(dict(kind="P1", day="2026-11-26", actor="Analyst", approval=True))
    run, = engine.claim()
    with engine.tx() as db:
        db.conn.execute("UPDATE run SET heartbeat_at=datetime('now','-31 seconds') WHERE run_id=?", (run["run_id"],))
    engine.claim(maintenance=True)
    engine.claim(maintenance=True)
    with engine.tx() as db:
        assert db.get_runs_by_status("failed")[0]["run_id"] == run["run_id"]
        assert db.conn.execute("SELECT count(*) FROM event WHERE event_type='run_killed_stale'").fetchone()[0] == 1
        assert all(stage["status"] in ("failed","rejected") for stage in db.get_stage_requests_by_bp_request(bid))
        assert not db.get_locks_snapshot()


def test_replay_external_signal_after_stage_transition(engine):
    bid = engine.submit(dict(kind="P1", day="2026-11-26", actor="Analyst", approval=True))
    run, = engine.claim()
    engine.publish(bid, run["token"])
    with engine.tx() as db:
        rid = db.get_runs_waiting_external(bid)[0]["run_id"]
    with pytest.raises(ValueError):
        engine.external(rid, "Директор", "approve", "first")
    command = (rid, "Риск-менеджер", "approve", "first")
    receipt = engine.external(*command)
    assert engine.external(*command) == receipt
    with pytest.raises(ValueError):
        engine.external(rid, "Риск-менеджер", "reject", "first", "Ошибка")
    with engine.tx() as db:
        director, = db.get_runs_waiting_external(bid)
        assert director["run_id"] != rid
