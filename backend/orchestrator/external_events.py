"""Role-bound, idempotent approval commands."""
import json
from .release import Release
from .admit import Admit

ROLES = {"approval":"Риск-менеджер","sign-off":"Директор"}


def previous_signal(db, event_id):
    row = db.conn.execute("SELECT payload FROM event WHERE event_type='external_signal' AND json_extract(payload,'$.event_id')=?", (event_id,)).fetchone()
    return json.loads(row[0]) if row else None


def receive_signal(db, run_id, actor, decision, event_id, comment, rerun):
    with db.transaction():
        if not isinstance(event_id,str) or not event_id.strip() or decision not in ("approve","reject") or not isinstance(comment,str):
            raise ValueError("Нужны ID события и корректное решение.")
        if decision=="reject" and not comment.strip():
            raise ValueError("Укажите причину отклонения.")
        command = dict(event_id=event_id,run_id=run_id,actor=actor,decision=decision,comment=comment)
        old = previous_signal(db,event_id)
        if old:
            if any(old.get(k)!=v for k,v in command.items()):
                raise ValueError("ID события уже использован.")
            return old
        row = db.conn.execute("""SELECT r.status,s.stage_type,sr.bp_request_id,b.initiator_id
            FROM run r JOIN stage_request sr USING(stage_request_id) JOIN stage s USING(stage_id)
            JOIN bp_request b USING(bp_request_id) WHERE run_id=?""",(run_id,)).fetchone()
        if not row or row["status"]!="waiting_external" or ROLES.get(row["stage_type"])!=actor:
            raise ValueError("Решение недоступно для текущего этапа.")
        if row["initiator_id"]==actor:
            raise ValueError("Инициатор не может согласовать собственную заявку.")
        jid = row["bp_request_id"]
        db.update_run_external_event(run_id,command)
        Release(db).release_run(run_id,"done" if decision=="approve" else "rejected")
        if decision == "approve":
            for sr in db.get_stage_requests_by_bp_request(jid):
                stage = db.get_stage(sr["stage_id"])
                if stage["stage_type"] in ROLES and sr["status"] == "queued":
                    Admit(db).one(sr["stage_request_id"])
        command.update(job=jid, successor=rerun(jid) if decision=="reject" else None,
                       message=actor+(" · согласовано" if decision=="approve" else " · отклонено"))
        db.log_event("external_signal","run",run_id,command,actor)
        return command
