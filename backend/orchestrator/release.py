"""Finalize one execution and propagate its outcome to dependent requests."""
from collections import deque


class Release:
    def __init__(self, db):
        self.db = db

    def release_run(self, run_id, status):
        db = self.db
        with db.transaction():
            run = db.conn.execute("""SELECT r.status,sr.stage_request_id,sr.bp_request_id FROM run r
                JOIN stage_request sr USING(stage_request_id) WHERE run_id=?""",(run_id,)).fetchone()
            if not run or run["status"] not in ("running","waiting_external","queued"):
                return False
            sid, bid = run["stage_request_id"], run["bp_request_id"]
            db.finish_run(run_id,status)
            db.release_locks(run_id)
            db.update_stage_request_status(sid,status)
            mirrors = db.conn.execute("SELECT stage_request_id,bp_request_id FROM stage_request WHERE merged_into_stage_request_id=?", (sid,)).fetchall()
            completed = [(sid,bid), *[(r[0],r[1]) for r in mirrors]]
            for request, owner in completed:
                db.update_stage_request_status(request,status)
                if status == "done":
                    db.wake_children(request)
                    unfinished = db.conn.execute("SELECT 1 FROM stage_request WHERE bp_request_id=? AND status<>'done' LIMIT 1",(owner,)).fetchone()
                    db.conn.execute("UPDATE bp_request SET status=? WHERE bp_request_id=? AND status NOT IN ('failed','cancelled','rejected')",("expanded" if unfinished else "done",owner))
                else:
                    db.conn.execute("UPDATE bp_request SET status=? WHERE bp_request_id=?",(status,owner))
                    pending = deque([request])
                    while pending:
                        for child in db.get_stage_requests_by_parent(pending.popleft()):
                            if child["status"] not in ("waiting_dependency","created","queued","waiting_resource"):
                                continue
                            cid = child["stage_request_id"]
                            db.conn.execute("UPDATE stage_request SET status='rejected',rejection_reason='parent_failed' WHERE stage_request_id=?",(cid,))
                            db.conn.execute("UPDATE bp_request SET status='rejected' WHERE bp_request_id=? AND status IN ('pending','expanded','running')",(child["bp_request_id"],))
                            pending.append(cid)
            db.log_event("run_released","run",run_id,dict(job=bid,status=status,message="Завершение этапа · ресурсы освобождены"))
            return True
