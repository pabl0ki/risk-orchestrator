"""Queue admission and periodic reconciliation of leases."""
from .admit import Admit
from .release import Release

SWEEP_INTERVAL = 300


def sweep_queue(db, maintenance=True):
    with db.transaction():
        if maintenance:
            for row in db.get_expired_locks():
                db.log_event("lock_expired","lock",row["lock_id"],{"run_id":row["run_id"]})
            db.expire_locks()
        for run in db.get_stale_runs(30):
            if Release(db).release_run(run["run_id"],"failed"):
                db.log_event("run_killed_stale","run",run["run_id"],{"message":"Исполнитель перестал отвечать"})
        admit = Admit(db)
        started = []
        for request in db.get_all_queued_requests():
            result = admit.admit_stage_request(request["stage_request_id"])
            if result["decision"] == "running":
                started.append(result)
        return started
