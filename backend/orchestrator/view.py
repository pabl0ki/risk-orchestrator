"""Read-only frontend projection of the seven relational tables."""
import json
from datetime import datetime
from .admit import required_locks


def epoch(value):
    if value is None:
        return None
    if isinstance(value,(int,float)):
        return value
    return datetime.fromisoformat(value).timestamp()


def snapshot(db):
    jobs, requests, events, batches, signals = {}, [], [], {}, {}
    stages = db.conn.execute("""SELECT sr.*,s.stage_type,s.required_resources,r.run_id,r.status AS run_status,
        r.started_at,r.finished_at,r.external_event FROM stage_request sr JOIN stage s USING(stage_id)
        LEFT JOIN run r USING(stage_request_id) ORDER BY sr.stage_request_id""")
    by_process = {}
    by_run = {}
    by_stage = {}
    for row in stages:
        bid = row["bp_request_id"]
        by_process.setdefault(bid,[]).append(row)
        by_stage[row["stage_request_id"]] = bid
        if row["run_id"]:
            by_run[row["run_id"]] = bid
    for b in db.conn.execute("""SELECT b.*,p.risk_type FROM bp_request b JOIN business_process p USING(business_process_id)
                                WHERE b.status<>'merged' ORDER BY bp_request_id"""):
        bid = b["bp_request_id"]
        params = json.loads(b["input_payload"] or "{}")
        route = by_process.get(bid,[])
        calc = next((r for r in route if r["stage_type"]=="calculation"),None)
        current = next((r for r in route if r["status"] not in ("done","merged")),None)
        wait = json.loads(current["waiting_for"] or "{}") if current else {}
        status = {"expanded":"queued","pending":"queued"}.get(b["status"],b["status"])
        locks = required_locks(calc,b["date"],params) if calc else []
        decision = next((json.loads(r["external_event"]) for r in route if r["stage_type"]=="approval" and r["external_event"]),{})
        approval_status = ("reviewed" if current and current["stage_type"]=="sign-off" else "pending") if status=="review" else (
            "approved" if decision and status=="done" else status)
        jobs[bid] = dict(id=bid,kind=b["risk_type"],day=b["date"],actor=b["initiator_id"],channel=b["initiator_type"],
            factor=params.get("factor",1),parent=params.get("parent",0),approval=bool(params.get("approval")),
            rerun=bool(params.get("rerun")),status=status,approval_status=approval_status,created=epoch(b["submitted_at"]),
            start=epoch(calc["started_at"]) if calc else None,calculated_at=epoch(calc["finished_at"]) if calc else None,
            end=max((epoch(r["finished_at"]) for r in route if r["finished_at"]),default=None),
            resources=[[l["resource_id"],"*" if l["data_key"]=="ALL" else l["data_key"],"R" if l["mode"]=="read" else "W"] for l in locks],
            reason=wait.get("message","") if isinstance(wait,dict) else "",wait_for=wait.get("wait_for") if isinstance(wait,dict) else None,
            external_run_id=current["run_id"] if current and status=="review" else None,
            assessment=dict(reviewed_by=decision.get("actor"),comment=decision.get("comment","")),
            inputs={},result=None,attempt=int(bool(calc and calc["run_id"])),token=None,lease=None)
    schedule = None
    for e in db.conn.execute("SELECT * FROM event ORDER BY event_id"):
        payload = json.loads(e["payload"] or "{}")
        bid = payload.get("job")
        if bid is None:
            bid = {"run":by_run,"stage_request":by_stage}.get(e["entity_type"],{}).get(e["entity_id"],0)
            if e["entity_type"]=="bp_request":
                bid = e["entity_id"]
        kind = e["event_type"]
        if kind in ("schedule_changed","schedule_fired"):
            schedule = payload if "due" in payload else None
        if kind=="request_received":
            requests.append(dict(key=payload["key"],job=bid,actor=e["actor"],channel=payload["channel"]))
        elif kind=="demo_batch":
            batches[e["entity_id"]] = payload
        elif kind=="external_signal":
            signals[payload["event_id"]] = [bid,payload["decision"],e["actor"],payload["successor"],payload["comment"]]
            successor = payload["successor"]
            if successor and bid in jobs and successor in jobs:
                jobs[bid]["rerun_to"] = successor
                jobs[successor]["rerun_from"] = bid
                for batch in batches.values():
                    if bid in batch["jobs"]:
                        batch["jobs"].append(successor)
                        batch["requests"] += 1
        events.append(dict(seq=e["event_id"],at=epoch(e["timestamp"]),job=bid,message=payload.get("message",kind)))
    routes = {bp["risk_type"]: json.loads(bp["route"] or "[]") for bp in db.get_all_business_processes()}
    return dict(jobs=list(jobs.values()),requests=requests,events=events,results={},heads={},routes=routes,
                signals=signals,demo_batches=list(batches.values()),schedule=schedule)
