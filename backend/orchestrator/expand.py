"""Expand a process template into a DAG of requests in one transaction."""
import json
from backend.database.utils import calculate_hash, validate_dag


class Expand:
    def __init__(self, db):
        self.db = db

    def expand_bp_request(self, bp_request_id):
        with self.db.transaction():
            db = self.db
            row = db.conn.execute("SELECT * FROM bp_request WHERE bp_request_id=?", (bp_request_id,)).fetchone()
            if not row:
                raise ValueError("Заявка не найдена.")
            if row["status"] != "pending":
                return {"status": row["status"], "stage_requests": db.get_stage_requests_by_bp_request(bp_request_id)}
            params = json.loads(row["input_payload"] or "{}")
            stages = [dict(stage) for stage in db.get_stages(row["business_process_id"])]
            route = json.loads(db.get_business_process(row["business_process_id"])["route"] or "[]")
            if isinstance(route,list) and route:
                by_id = {s["stage_id"]:s for s in stages}
                stages = [by_id[sid] for sid in route] if all(sid in by_id for sid in route) else []
            for stage in stages:
                stage["depends_on_stages"] = json.loads(stage["depends_on_stages"] or "[]")
            valid, error = validate_dag(stages)
            if not valid or not stages:
                reason = "circular_dependency" if error and "циклическая зависимость" in error else "invalid_route"
                db.conn.execute("UPDATE bp_request SET status='rejected' WHERE bp_request_id=?", (bp_request_id,))
                db.log_event("bp_request_rejected","bp_request",bp_request_id,{"reason":reason})
                return {"status":"rejected","reason":reason}

            if params.get("approval") is False:
                stages = [s for s in stages if s["stage_type"] not in ("approval","sign-off")]
            created = {}
            pending = {s["stage_id"]:s for s in stages}
            while pending:
                for sid, stage in list(pending.items()):
                    parents = stage["depends_on_stages"]
                    if any(parent in pending for parent in parents):
                        continue
                    dependencies = [created[parent] for parent in parents if parent in created]
                    if not dependencies and params.get("parent"):
                        previous = db.get_stage_requests_by_bp_request(params["parent"])
                        dependencies.extend(s["stage_request_id"] for s in previous)
                    stage_params = params if stage["stage_type"] not in ("approval","sign-off") else dict(params, subject=bp_request_id)
                    created[sid] = db.create_stage_request(bp_request_id,sid,row["date"],
                        stage["priority_default"] or 10, calculate_hash(sid,row["date"],stage_params),dependencies)
                    del pending[sid]
            db.conn.execute("UPDATE bp_request SET status='expanded' WHERE bp_request_id=?", (bp_request_id,))
            return {"status":"expanded","stage_requests":db.get_stage_requests_by_bp_request(bp_request_id)}
