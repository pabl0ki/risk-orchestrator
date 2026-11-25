"""One-time import of orchestration metadata, without importing model data."""
import json
import sqlite3
import tempfile
from pathlib import Path
from contextlib import closing
from backend.database.utils import calculate_hash
from .expand import Expand
from .external_events import ROLES


def migrate(db):
    path = Path(db.db_path)
    with tempfile.NamedTemporaryFile(prefix=path.stem+"-before-relational-",suffix=".sqlite3",
                                     dir=path.parent,delete=False) as file:
        backup = file.name
    with closing(sqlite3.connect(path.resolve().as_uri()+"?mode=ro",uri=True)) as src, closing(sqlite3.connect(backup)) as dst:
        src.backup(dst)
        if dst.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("Не удалось проверить резервную копию.")
    row = db.conn.execute("SELECT payload FROM ledger WHERE id=1").fetchone()
    old = json.loads(row[0]) if row else {}
    decisions = {(sig[0],sig[2]):(key,sig) for key,sig in old.get("signals",{}).items()}
    for job in old.get("jobs",[]):
        jid = job["id"]
        pid = db.conn.execute("SELECT business_process_id FROM business_process WHERE risk_type=?",(job["kind"],)).fetchone()[0]
        params = dict(parent=job.get("parent",0),factor=job.get("factor",1),approval=bool(job.get("approval")),
                      rerun=bool(job.get("rerun")),shared=any(r[0]=="work" for r in job.get("resources",[])))
        if params["rerun"]:
            params["rerun_key"] = str(jid)
        day,status = job["day"],job["status"]
        db.conn.execute("""INSERT INTO bp_request(bp_request_id,business_process_id,date,initiator_id,initiator_type,
                        input_payload,input_hash,submitted_at,status) VALUES(?,?,?,?,?,?,?,?,'pending')""",
                        (jid,pid,day,job["actor"],job["channel"],json.dumps(params),
                         calculate_hash(pid,day,params),job["created"]))
        Expand(db).expand_bp_request(jid)
        for sr in db.get_stage_requests_by_bp_request(jid):
            sid = sr["stage_request_id"]
            kind = db.get_stage(sr["stage_id"])["stage_type"]
            if status in ("queued","running"):
                continue  # The previous executor no longer owns a lease.
            signal = decisions.get((jid,ROLES.get(kind)))
            if kind=="calculation":
                outcome = "done" if job.get("calculated_at") or status in ("done","review") else status
            elif signal:
                outcome = "done" if signal[1][1]=="approve" else "rejected"
            elif status=="review":
                if kind=="sign-off" and (jid,"Риск-менеджер") not in decisions:
                    continue
                outcome = "waiting_external"
            else:
                outcome = "done" if status=="done" else status
            rid = db.create_run(sid)
            db.update_stage_request_status(sid,"approved" if outcome=="waiting_external" else outcome)
            db.update_run_status(rid,outcome,started_at=job.get("start"),finished_at=(
                job.get("calculated_at",job.get("end")) if kind=="calculation" else
                job.get("end") if outcome!="waiting_external" else None))
            if signal:
                key,sig = signal
                receipt = dict(event_id=key,job=jid,run_id=rid,decision=sig[1],actor=sig[2],
                               successor=sig[3],comment=sig[4] if len(sig)>4 else "")
                db.update_run_external_event(rid,receipt)
                db.log_event("external_signal","run",rid,receipt,sig[2])
        db.conn.execute("UPDATE bp_request SET status=? WHERE bp_request_id=?",
                        ("expanded" if status in ("running","queued") else status,jid))
    for r in old.get("requests",[]):
        kind,day,factor,actor,channel,shared,rerun,parent,approval = r["signature"]
        params = dict(parent=parent,factor=factor,approval=approval,rerun=rerun,shared=shared)
        db.log_event("request_received","bp_request",r["job"],dict(job=r["job"],key=r["key"],
            signature=[kind,day,actor,channel,params],channel=channel,message="Зарегистрировано обращение"),actor)
    for e in old.get("events",[]):
        db.conn.execute("""INSERT INTO event(timestamp,event_type,entity_type,entity_id,payload,actor)
                        VALUES(?,'legacy_operation','bp_request',?,?,'system')""",
                        (e["at"],e["job"],json.dumps(dict(job=e["job"],message=e["message"]))))
    for batch in old.get("demo_batches",[]):
        db.log_event("demo_batch","batch",batch["id"],batch)
    if old.get("schedule"):
        db.log_event("schedule_changed","system",0,old["schedule"])
    if db.conn.execute("PRAGMA foreign_key_check").fetchone():
        raise ValueError("Перенос прерван: нарушены связи между таблицами.")
    db.conn.execute("DROP TABLE ledger")
