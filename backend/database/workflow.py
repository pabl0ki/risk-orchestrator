"""Relational storage for the orchestrator's transactional scheduling view."""
import json
from datetime import datetime, timezone

from .utils import calculate_hash

STAGES = {
    "calculation": "Расчёт", "approval": "Согласование",
    "sign-off": "Утверждение", "publication": "Публикация",
}
PARAMETERS = ("factor", "resources", "parent", "route", "approval", "rerun")
BP_STATUS = {"queued": "expanded", "running": "in_progress", "review": "in_progress",
             "done": "done", "rejected": "rejected", "cancelled": "cancelled", "failed": "failed"}


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def timestamp(value):
    if value is not None:
        return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def read_events(con):
    return [json.loads(row[0]) for row in con.execute(
        """SELECT payload FROM event WHERE event_type='orchestrator.operation'
           OR event_id IN (SELECT event_id FROM workflow_event) ORDER BY event_id""")]


def read_state(con):
    state = dict(jobs=[], requests=[], results={}, heads={}, events=read_events(con),
                 routes={}, signals={}, schedule=None)
    for row in con.execute("SELECT code,revision FROM process_route"):
        state["routes"][row["code"]] = row["revision"]
    jobs = con.execute("""
        SELECT w.*, b.date,b.initiator_type,b.initiator_id,b.input_payload,b.submitted_at,
               b.status,p.code
        FROM workflow w JOIN bp_request b USING(bp_request_id)
        JOIN process_route p USING(business_process_id) ORDER BY public_id
    """)
    for r in jobs:
        status = r["status"]
        if status == "expanded":
            status = "queued"
        elif status == "in_progress":
            status = "review" if r["result_version"] else "running"
        job = dict(json.loads(r["input_payload"]), id=r["public_id"], kind=r["code"],
                   day=r["date"], actor=r["initiator_id"], channel=r["initiator_type"],
                   created=r["submitted_at"], status=status, start=r["start_at"], end=r["end_at"],
                   attempt=r["attempt"], token=r["token"], lease=r["lease"],
                   inputs=json.loads(r["inputs"]), result=r["result_version"], reason=r["reason"])
        if r["has_wait_for"]:
            job["wait_for"] = r["wait_for"]
        if r["calculated_at"] is not None:
            job["calculated_at"] = r["calculated_at"]
        state["jobs"].append(job)
    for r in con.execute("""SELECT rr.*,b.initiator_id,b.initiator_type FROM request_receipt rr
                            JOIN bp_request b USING(bp_request_id) ORDER BY rr.bp_request_id"""):
        j = state["jobs"][r["public_id"] - 1]
        actor, channel = r["initiator_id"], r["initiator_type"]
        signature = [j["kind"], j["day"], j["factor"], actor, channel,
                     any(name == "work" for name, _, _ in j["resources"]),
                     j["rerun"], j["parent"], j["approval"]]
        state["requests"].append(dict(key=r["request_key"], signature=signature,
                                      job=j["id"], actor=actor, channel=channel))
    for r in con.execute("SELECT * FROM result_version ORDER BY rowid"):
        state["results"][r["version"]] = dict(id=r["version"], object=r["resource"], scope=r["scope"],
            previous=r["previous"], rows=json.loads(r["rows_json"]), inputs=json.loads(r["inputs"]),
            token=r["token"], algorithm=r["algorithm"], factor=r["factor"], approval=r["approval"])
        if r["scope"] is None:
            del state["results"][r["version"]]["scope"]
    state["heads"] = dict(con.execute("SELECT resource_key,version FROM published_result"))
    for r in con.execute("SELECT * FROM decision_receipt ORDER BY rowid"):
        state["signals"][r["command_key"]] = [r["public_id"], r["decision"], r["actor"],
            r["successor"], r["comment"], r["version"]][:r["signature_length"]]
    plan = con.execute("SELECT process_code AS kind,business_date AS day,period,due FROM schedule").fetchone()
    if plan:
        state["schedule"] = dict(plan)
    for r in con.execute("SELECT * FROM demo_batch ORDER BY batch_id"):
        jobs = con.execute("SELECT public_id FROM demo_batch_job WHERE batch_id=? ORDER BY position",
                           (r["batch_id"],))
        state.setdefault("demo_batches", []).append(dict(id=r["batch_id"], day=r["business_date"],
            scenario=r["scenario"], requests=r["requests"], jobs=[item[0] for item in jobs]))
    return state


def write_route(con, code, revision, catalog):
    row = con.execute("SELECT business_process_id FROM process_route WHERE code=?", (code,)).fetchone()
    if row:
        con.execute("UPDATE process_route SET revision=? WHERE code=?", (revision, code))
        return
    name, resources = catalog[code]
    pid = con.execute("INSERT INTO business_process(name,risk_type,created_at) VALUES(?,?,CURRENT_TIMESTAMP)",
                      (name, code)).lastrowid
    route = []
    for stage_type, title in STAGES.items():
        required = list(resources)
        if code in ("P1", "P2"):
            required.append(("reference", "R"))
        declarations = [dict(resource_id=k, mode="read" if m=="R" else "write",
                             granularity="table" if k=="reference" else "partition") for k,m in required]
        sid = con.execute("""INSERT INTO stage(business_process_id,name,stage_type,required_resources,
                             depends_on_stages) VALUES(?,?,?,?,?)""",
                          (pid,title,stage_type,encode(declarations if not route else []),encode(route[-1:]))).lastrowid
        route.append(sid)
    con.execute("UPDATE business_process SET route=? WHERE business_process_id=?", (encode(route),pid))
    con.execute("INSERT INTO process_route VALUES(?,?,?)", (code,pid,revision))


def register_request(con, request, job, submitted_at):
    jid = job["id"]
    owner = con.execute("SELECT bp_request_id FROM workflow WHERE public_id=?", (jid,)).fetchone()
    pid = con.execute("SELECT business_process_id FROM process_route WHERE code=?", (job["kind"],)).fetchone()[0]
    params = {key: job[key] for key in PARAMETERS}
    digest = calculate_hash(pid, job["day"], params)
    bid = con.execute("""INSERT INTO bp_request(business_process_id,date,initiator_type,initiator_id,
        input_payload,input_hash,submitted_at,status,merged_into_bp_request_id)
        VALUES(?,?,?,?,?,?,?,?,?)""", (pid,job["day"],request["channel"],request["actor"],encode(params),
        digest,submitted_at,"merged" if owner else "expanded",owner[0] if owner else None)).lastrowid
    if not owner:
        con.execute("""INSERT INTO workflow(public_id,bp_request_id,attempt,lease,inputs,reason,has_wait_for)
                       VALUES(?,?,0,0,'{}','',0)""", (jid,bid))
        parent_ids = []
        if job["parent"]:
            parent = con.execute("""SELECT sr.stage_request_id FROM workflow w JOIN stage_request sr
                USING(bp_request_id) JOIN stage s USING(stage_id)
                WHERE public_id=? AND s.stage_type='publication'""", (job["parent"],)).fetchone()
            parent_ids = [parent[0]]
        for stage in con.execute("SELECT stage_id,stage_type FROM stage WHERE business_process_id=? ORDER BY stage_id",(pid,)):
            if not job["approval"] and stage["stage_type"] in ("approval","sign-off"):
                continue
            sr = con.execute("""INSERT INTO stage_request(bp_request_id,stage_id,date,priority,input_hash,
                submitted_at,status,parent_stage_request_ids) VALUES(?,?,?,?,?,?,?,?)""",
                (bid,stage["stage_id"],job["day"],0,calculate_hash(stage["stage_id"],job["day"],params),
                 job["created"],"waiting_dependency" if parent_ids else "queued",encode(parent_ids))).lastrowid
            parent_ids = [sr]
    con.execute("INSERT INTO request_receipt VALUES(?,?,?)", (request["key"],bid,jid))


def stage_states(job, decisions):
    status = job["status"]
    calc = "done" if job["result"] else status
    yield "calculation", calc, None
    blocked = status in ("cancelled","failed","rejected")
    ready = calc == "done"
    for stage, actor in (("approval","Риск-менеджер"), ("sign-off","Директор")):
        if not job["approval"]:
            break
        receipt = decisions.get(actor)
        if receipt:
            outcome = "done" if receipt[1] == "approve" else "rejected"
        elif ready and status == "review":
            outcome = "waiting_external"
        else:
            outcome = "rejected" if blocked else "waiting_dependency"
        yield stage, outcome, receipt
        ready = outcome == "done"
    yield "publication", "done" if status=="done" else ("rejected" if blocked else "waiting_dependency"), None


def update_job(con, job, decisions, decision_times):
    jid = job["id"]
    bid = con.execute("SELECT bp_request_id FROM workflow WHERE public_id=?", (jid,)).fetchone()[0]
    con.execute("UPDATE bp_request SET submitted_at=?,status=? WHERE bp_request_id=?",
                (job["created"],BP_STATUS[job["status"]],bid))
    con.execute("""UPDATE workflow SET attempt=?,token=?,lease=?,inputs=?,result_version=?,reason=?,
        start_at=?,end_at=?,calculated_at=?,wait_for=?,has_wait_for=? WHERE public_id=?""",
        (job["attempt"],job["token"],job["lease"],encode(job["inputs"]),job["result"],job["reason"],
         job["start"],job["end"],job.get("calculated_at"),job.get("wait_for"),"wait_for" in job,jid))
    stages = dict(con.execute("""SELECT s.stage_type,sr.stage_request_id FROM stage_request sr
                                 JOIN stage s USING(stage_id) WHERE sr.bp_request_id=?""", (bid,)))
    previous_finish = job.get("calculated_at")
    for kind, status, decision in stage_states(job, decisions):
        sr = stages[kind]
        wait = job["reason"] if kind=="calculation" and status=="queued" else ""
        sr_status = {"waiting_external":"approved","running":"approved","done":"approved",
                     "cancelled":"rejected","failed":"rejected"}.get(status,status)
        if wait and job.get("parent") and job.get("wait_for")==job["parent"]:
            sr_status = "waiting_dependency"
        con.execute("UPDATE stage_request SET status=?,waiting_for=?,rejection_reason=? WHERE stage_request_id=?",
                    (sr_status,encode(dict(reason=wait,job=job.get("wait_for"))) if wait else None,
                     job["reason"] if sr_status=="rejected" else None,sr))
        existing = con.execute("SELECT run_id FROM run WHERE stage_request_id=?", (sr,)).fetchone()
        if not existing and (status in ("queued","waiting_dependency") or
                             (status in ("rejected","cancelled","failed") and not decision and
                              (kind!="calculation" or not job["attempt"]))):
            continue
        rid = existing[0] if existing else con.execute("""INSERT INTO run(stage_request_id,status,created_at)
            VALUES(?,?,?)""", (sr,"queued",timestamp(job["created"]))).lastrowid
        run_status = "cancelled" if status=="rejected" else status
        calc = kind=="calculation"
        start = job["start"] if calc else previous_finish
        if calc:
            finish = job.get("calculated_at") or job["end"]
        elif decision:
            finish = decision_times.get((jid, decision[2]), job["end"])
            previous_finish = finish
        else:
            finish = job["end"]
        external = (dict(actor=decision[2],decision=decision[1],comment=decision[4] if len(decision)>4 else "")
                    if decision else None)
        con.execute("""UPDATE run SET status=?,started_at=?,finished_at=CASE
            WHEN ? IN ('done','cancelled','failed') THEN COALESCE(finished_at,?) ELSE NULL END,
            lease_expires_at=?,external_event=? WHERE run_id=?""",
            (run_status,timestamp(start),run_status,timestamp(finish),timestamp(job["lease"]) if calc and
             status=="running" else None,encode(external) if external else None,rid))
        if calc:
            sync_locks(con, rid, job)


def sync_locks(con, rid, job):
    if job["status"] != "running":
        con.execute("""UPDATE lock SET state='released',released_at=CURRENT_TIMESTAMP
                       WHERE run_id=? AND state='held'""", (rid,))
        return
    con.execute("UPDATE run SET heartbeat_at=CURRENT_TIMESTAMP WHERE run_id=?", (rid,))
    con.execute("""UPDATE lock SET state='expired',released_at=expires_at
                   WHERE run_id=? AND state='held' AND acquired_at<>?""",(rid,timestamp(job["start"])))
    for name, scope, mode in job["resources"]:
        con.execute("""INSERT INTO lock(resource_id,granularity,data_key,run_id,mode,acquired_at,expires_at,state)
            VALUES(?,?,?,?,?,?,?,'held') ON CONFLICT(run_id,resource_id,granularity,data_key)
            DO UPDATE SET state='held',expires_at=excluded.expires_at,acquired_at=excluded.acquired_at,
                          released_at=NULL""",(name,"table" if scope=="*" else "partition",
            "ALL" if scope=="*" else scope,rid,"read" if mode=="R" else "write",
            timestamp(job["start"]),timestamp(job["lease"])))


def save_changes(con, old, state, catalog):
    for code, revision in state["routes"].items():
        if revision != old["routes"].get(code):
            write_route(con, code, revision, catalog)
    new_events = state["events"][len(old["events"]):]
    received = {entry["request_key"]:entry["at"] for entry in new_events if "request_key" in entry}
    for request in state["requests"][len(old["requests"]):]:
        job = state["jobs"][request["job"]-1]
        register_request(con, request, job, received.get(request["key"],job["created"]))
    decisions = {}
    decision_times = {}
    for entry in state["events"]:
        if entry.get("type") in ("approval_granted","approval_rejected"):
            decision_times[entry["job"],entry["actor"]] = entry["at"]
        elif " · согласовано " in entry["message"] or " · отклонено " in entry["message"]:
            decision_times[entry["job"],entry["message"].split(" · ",1)[0]] = entry["at"]
    for key, receipt in state["signals"].items():
        decisions.setdefault(receipt[0], {})[receipt[2]] = receipt
        if key not in old["signals"]:
            con.execute("INSERT INTO decision_receipt VALUES(?,?,?,?,?,?,?,?)",
                        (key, *(receipt+[None]*6)[:6], len(receipt)))
    for index, job in enumerate(state["jobs"]):
        if index >= len(old["jobs"]) or job != old["jobs"][index]:
            update_job(con, job, decisions.get(job["id"], {}), decision_times)
    owners = {job["result"]: job["id"] for job in state["jobs"] if job["result"]}
    for key, version in state["results"].items():
        if version == old["results"].get(key):
            continue
        con.execute("""INSERT INTO result_version VALUES(?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(version) DO UPDATE SET rows_json=excluded.rows_json,approval=excluded.approval""",
            (key,owners[key],version["object"],version.get("scope"),version.get("previous"),
             encode(version["rows"]),encode(version["inputs"]),version["algorithm"],version["factor"],
             version["token"],version["approval"]))
    for key, version in state["heads"].items():
        if version != old["heads"].get(key):
            con.execute("""INSERT INTO published_result VALUES(?,?) ON CONFLICT(resource_key)
                           DO UPDATE SET version=excluded.version""", (key,version))
    if state["schedule"] != old["schedule"]:
        con.execute("DELETE FROM schedule")
        if state["schedule"]:
            con.execute("INSERT INTO schedule VALUES(1,:kind,:day,:period,:due)", state["schedule"])
    for index, batch in enumerate(state.get("demo_batches", [])):
        previous = old.get("demo_batches", [])
        if index < len(previous) and batch == previous[index]:
            continue
        con.execute("""INSERT INTO demo_batch VALUES(:id,:scenario,:day,:requests)
            ON CONFLICT(batch_id) DO UPDATE SET requests=excluded.requests""", batch)
        con.executemany("INSERT OR IGNORE INTO demo_batch_job VALUES(?,?,?)",
                        [(batch["id"],jid,i) for i,jid in enumerate(batch["jobs"])])
    for entry in new_events:
        if "request_key" in entry:
            owner = con.execute("SELECT bp_request_id FROM request_receipt WHERE request_key=?",
                                (entry["request_key"],)).fetchone()
        else:
            owner = con.execute("SELECT bp_request_id FROM workflow WHERE public_id=?", (entry["job"],)).fetchone()
        eid = con.execute("""INSERT INTO event(timestamp,event_type,entity_type,entity_id,payload,actor)
                           VALUES(?,?,?,?,?,?)""",
                          (timestamp(entry["at"]), entry.get("type", "orchestrator.operation"),
                           "bp_request" if owner else "system", owner[0] if owner else 0,
                           encode(entry), entry.get("actor", "system"))).lastrowid
        con.execute("INSERT INTO workflow_event VALUES(?,?)", (eid, entry["seq"]))

