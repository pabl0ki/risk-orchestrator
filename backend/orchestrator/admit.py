"""Deduplication, dependencies, compatibility and atomic admission."""
import json
import time

SLOTS = 4
LABELS = dict(deals="Сделки", rates="Курсы", credit="Кредитный результат",
              market="Рыночный результат", reference="Справочник", work="Общая рабочая таблица")


def required_locks(stage, day, params):
    locks = []
    for spec in json.loads(stage["required_resources"] or "[]"):
        mode = {"R":"read","W":"write"}.get(spec["mode"],spec["mode"])
        if mode not in ("read","write","exclusive"):
            raise ValueError("Некорректный режим доступа.")
        table = spec.get("granularity") == "table"
        locks.append(dict(resource_id=spec["resource_id"], granularity="table" if table else "partition",
                          data_key="ALL" if table else spec.get("data_key") or day, mode=mode))
    if params.get("shared") and stage["stage_type"] == "calculation":
        locks.append(dict(resource_id="work",granularity="table",data_key="ALL",mode="write"))
    return locks


class Admit:
    def __init__(self, db):
        self.db = db

    def wait(self, sr, status, message, blocker=None):
        info = dict(message=message,wait_for=blocker)
        if json.loads(sr["waiting_for"] or "null") != info:
            self.db.update_stage_request_status(sr["stage_request_id"],status,info)
            self.db.log_event(status,"bp_request",sr["bp_request_id"],info)
        return {"decision":status}

    def one(self, sr_id):
        db = self.db
        with db.transaction():
            sr = db.conn.execute("SELECT * FROM stage_request WHERE stage_request_id=?", (sr_id,)).fetchone()
            if not sr or sr["status"] not in ("created","queued","waiting_dependency","waiting_resource"):
                return {"decision":"skip"}
            duplicate = db.conn.execute("""SELECT stage_request_id FROM stage_request WHERE stage_id=? AND date=?
                AND input_hash=? AND status='approved' AND stage_request_id<>?""",
                (sr["stage_id"],sr["date"],sr["input_hash"],sr_id)).fetchone()
            if duplicate:
                db.conn.execute("UPDATE stage_request SET status='merged',merged_into_stage_request_id=? WHERE stage_request_id=?",
                                (duplicate[0],sr_id))
                return {"decision":"merged"}
            ready, unfinished = db.check_dependencies(sr_id)
            if not ready:
                parent = db.conn.execute("SELECT bp_request_id,status FROM stage_request WHERE stage_request_id=?", (unfinished[0],)).fetchone()
                if not parent or parent["status"] in ("failed","cancelled","rejected"):
                    db.conn.execute("UPDATE stage_request SET status='rejected',rejection_reason='parent_failed' WHERE stage_request_id=?", (sr_id,))
                    db.conn.execute("UPDATE bp_request SET status='rejected' WHERE bp_request_id=?", (sr["bp_request_id"],))
                    return {"decision":"rejected"}
                return self.wait(sr,"waiting_dependency",f"Ожидание предшественника №{parent[0]}",parent[0])
            stage = db.get_stage(sr["stage_id"])
            params = json.loads(db.conn.execute("SELECT input_payload FROM bp_request WHERE bp_request_id=?", (sr["bp_request_id"],)).fetchone()[0])
            try:
                locks = required_locks(stage,sr["date"],params)
            except (ValueError,KeyError) as exc:
                db.conn.execute("UPDATE stage_request SET status='rejected',rejection_reason=? WHERE stage_request_id=?", (str(exc),sr_id))
                db.conn.execute("UPDATE bp_request SET status='rejected' WHERE bp_request_id=?", (sr["bp_request_id"],))
                return {"decision":"rejected"}
            for lock in locks:
                for held in db.get_active_locks(lock["resource_id"],lock["data_key"]):
                    if held["mode"] == lock["mode"] == "read":
                        continue
                    owner = db.conn.execute("SELECT bp_request_id FROM run JOIN stage_request USING(stage_request_id) WHERE run_id=?", (held["run_id"],)).fetchone()[0]
                    name = LABELS.get(lock["resource_id"],lock["resource_id"])
                    return self.wait(sr,"waiting_resource",f"{name} · ожидает №{owner}",owner)
            external = stage["stage_type"] in ("approval","sign-off")
            if not external and len(db.get_runs_by_status("running")) >= SLOTS:
                return self.wait(sr,"queued","Все исполнители заняты")
            rid = db.create_run(sr_id)
            if not db.acquire_locks(rid,locks):
                raise RuntimeError("Конфликт блокировок внутри транзакции допуска.")
            db.update_stage_request_status(sr_id,"approved")
            status = "waiting_external" if external else "running"
            db.update_run_status(rid,status,started_at=time.time())
            db.conn.execute("UPDATE bp_request SET status=? WHERE bp_request_id=?", ("review" if external else "running",sr["bp_request_id"]))
            if not external:
                db.update_heartbeat(rid)
            db.log_event("run_created","run",rid,dict(job=sr["bp_request_id"],message="Ожидает решения" if external else "Допуск · получены все ресурсы"))
            return {"decision":status,"id":sr["bp_request_id"],"run_id":rid,"token":str(rid)}
