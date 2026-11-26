"""Relational process orchestrator. Public job IDs identify process requests."""
import json
import time
import uuid
from contextlib import closing, contextmanager
from datetime import date

from backend.database.connection import OrchestratorDB
from backend.database.utils import calculate_hash
from .expand import Expand
from .admit import Admit
from .release import Release
from .sweep_queue import sweep_queue
from .external_events import receive_signal, previous_signal
from .view import snapshot

CATALOG = {
    "P1": ("Кредитный риск", [("deals", "R"), ("rates", "R"), ("credit", "W")]),
    "P2": ("Рыночный риск", [("deals", "R"), ("rates", "R"), ("market", "W")]),
    "Z1": ("Загрузка сделок", [("deals", "W"), ("rates", "R")]),
    "Z2": ("Загрузка курсов", [("rates", "W"), ("deals", "R")]),
    "FX": ("Обновление справочника", [("reference", "W")]),
}

LABELS = dict(
    deals="Сделки", rates="Курсы", credit="Кредитный результат",
    market="Рыночный результат", reference="Справочник", work="Общая рабочая таблица"
)

LEASE_SECONDS = 30
HEARTBEAT_INTERVAL = 10
STALE_THRESHOLD = 30
SLOTS = 4


def now():
    return time.time()


class Engine:
    def __init__(self, db_path):
        self.db_path = str(db_path)
        with closing(OrchestratorDB(self.db_path)) as db, db.transaction():
            for kind,(name,resources) in CATALOG.items():
                bp = db.get_business_process_by_name(name)
                pid = bp["business_process_id"] if bp else db.create_business_process(name,kind,[])
                stages = db.get_stages(pid)
                specs = list(resources)
                if kind in ("P1","P2"):
                    specs.append(("reference","R"))
                locks = [dict(resource_id=n,granularity="table" if n=="reference" else "partition",
                              mode="read" if m=="R" else "write") for n,m in specs]
                if not stages:
                    calc = db.create_stage(pid,"calculation",json.dumps(locks),None,4,10)
                elif len(stages)==1 and stages[0]["stage_type"]=="calculation":
                    calc = stages[0]["stage_id"]
                    db.conn.execute("UPDATE stage SET required_resources=? WHERE stage_id=?", (json.dumps(locks),calc))
                else:
                    continue
                review = db.create_stage(pid,"approval","[]",json.dumps([calc]),None,10)
                sign = db.create_stage(pid,"sign-off","[]",json.dumps([review]),None,10)
                db.update_business_process(pid,name,kind,[calc,review,sign])
            if db.conn.execute("SELECT 1 FROM sqlite_master WHERE name='ledger' AND type='table'").fetchone():
                from .legacy import migrate
                migrate(db)

    @contextmanager
    def tx(self):
        with closing(OrchestratorDB(self.db_path,initialize=False)) as db, db.transaction():
            yield db

    def snapshot(self):
        with closing(OrchestratorDB(self.db_path,initialize=False)) as db:
            db.conn.execute("BEGIN")
            return snapshot(db)

    def submit(self,data):
        with self.tx() as db:
            return self.add(db,data)

    def add(self,db,data):
        kind,day = data["kind"],date.fromisoformat(data["day"]).isoformat()
        actor,channel = str(data.get("actor","")).strip(),data.get("channel","manual")
        factor = int(data.get("factor",1))
        if kind not in CATALOG or not actor or len(actor)>80 or channel not in ("manual","schedule") or not 1<=factor<=10:
            raise ValueError("Проверьте параметры запуска.")
        parent = int(data.get("parent") or 0)
        if parent and not db.get_stage_requests_by_bp_request(parent):
            raise ValueError("Предшественник не найден.")
        key = data.get("key") or uuid.uuid4().hex
        params = dict(factor=factor,approval=bool(data.get("approval")),parent=parent,
                      shared=bool(data.get("shared")),rerun=bool(data.get("rerun")))
        signature = [kind,day,actor,channel,params.copy()]
        prior = db.conn.execute("SELECT payload FROM event WHERE event_type='request_received' AND json_extract(payload,'$.key')=?",(key,)).fetchone()
        if prior:
            saved = json.loads(prior[0])
            if saved["signature"] != signature:
                raise ValueError("Ключ обращения уже использован с другими параметрами.")
            return saved["job"]
        if params["rerun"]:
            params["rerun_key"] = key
        bp = db.get_business_process_by_name(CATALOG[kind][0])
        input_hash = calculate_hash(bp["business_process_id"], day, params)
        existing = db.conn.execute(
            "SELECT bp_request_id FROM bp_request WHERE business_process_id=? AND date=? AND input_hash=? "
            "AND status NOT IN ('done','failed','rejected','cancelled','merged')",
            (bp["business_process_id"], day, input_hash),
        ).fetchone()
        if existing:
            jid = existing["bp_request_id"]
            db.log_event("request_received","bp_request",jid,dict(job=jid,key=key,signature=signature,channel=channel,
                message="Присоединено обращение (дубликат)"+" · "+actor),actor)
            return jid
        receipt = db.create_bp_request(bp["business_process_id"],day,channel,actor,params)
        jid = receipt["id"]
        if not receipt["merged"]:
            Expand(db).expand_bp_request(jid)
        db.log_event("request_received","bp_request",jid,dict(job=jid,key=key,signature=signature,channel=channel,
            message=("Присоединено обращение" if receipt["merged"] else "Принято обращение")+" · "+actor),actor)
        return jid

    def schedule(self,data):
        plan = {"enabled":False}
        if data.get("enabled"):
            period = int(data["period"])
            if not 5 <= period <= 3600 or data["kind"] not in CATALOG:
                raise ValueError("Проверьте параметры расписания.")
            plan = dict(kind=data["kind"],day=date.fromisoformat(data["day"]).isoformat(),period=period,due=time.time()+period)
        with self.tx() as db:
            db.log_event("schedule_changed","system",0,plan)

    def queue_revision(self):
        with closing(OrchestratorDB(self.db_path,initialize=False)) as db:
            return db.conn.execute("SELECT COALESCE(MAX(event_id),0) FROM event").fetchone()[0]

    def claim(self,maintenance=False):
        with self.tx() as db:
            row = db.conn.execute("SELECT payload FROM event WHERE event_type IN ('schedule_changed','schedule_fired') ORDER BY event_id DESC LIMIT 1").fetchone()
            plan = json.loads(row[0]) if row else {}
            if plan.get("due",float("inf")) <= time.time():
                self.add(db,dict(**plan,actor="Регламент",channel="schedule"))
                plan["due"] = time.time()+plan["period"]
                db.log_event("schedule_fired","system",0,plan)
            return sweep_queue(db,maintenance)

    def _owned_run(self,db,jid,token):
        return db.conn.execute("""SELECT r.* FROM run r JOIN stage_request sr USING(stage_request_id)
            WHERE r.run_id=? AND sr.bp_request_id=? AND r.status='running'
            AND r.lease_expires_at>datetime('now')""",(token,jid)).fetchone()

    def heartbeat(self,jid,token):
        with self.tx() as db:
            return bool(self._owned_run(db,jid,token) and db.update_heartbeat(int(token)))

    def publish(self,jid,token,result=None):
        with self.tx() as db:
            if self._owned_run(db,jid,token):
                Release(db).release_run(int(token),"done")
                for sr in db.get_stage_requests_by_bp_request(jid):
                    if sr["status"] == "queued" and db.get_stage(sr["stage_id"])["stage_type"] in ("approval","sign-off"):
                        Admit(db).admit_stage_request(sr["stage_request_id"])
                return jid
            done = db.conn.execute("""SELECT 1 FROM run JOIN stage_request USING(stage_request_id)
                WHERE run_id=? AND bp_request_id=? AND run.status='done'""",(token,jid)).fetchone()
            return jid if done else None

    def _rerun(self,db,jid):
        b = db.conn.execute("""SELECT b.*,p.risk_type FROM bp_request b JOIN business_process p
            USING(business_process_id) WHERE bp_request_id=?""",(jid,)).fetchone()
        params = json.loads(b["input_payload"])
        params.update(kind=b["risk_type"],day=b["date"],actor=b["initiator_id"],rerun=True)
        return self.add(db,params)

    def external(self,run_id,actor,decision,event_id,comment=""):
        with self.tx() as db:
            return receive_signal(db,run_id,actor,decision,event_id,comment,lambda j:self._rerun(db,j))

    def decide(self,jid,decision,actor,event_id,comment="",version=None):
        with self.tx() as db:
            old = previous_signal(db,event_id)
            if old:
                if old["job"]!=jid:
                    raise ValueError("ID события уже использован.")
                rid = old["run_id"]
            else:
                waiting = db.get_runs_waiting_external(jid)
                if not waiting:
                    raise ValueError("Запуск не ожидает решения.")
                rid = waiting[0]["run_id"]
            return receive_signal(db,rid,actor,decision,event_id,comment,lambda j:self._rerun(db,j))["successor"]

    def control(self,jid,action):
        with self.tx() as db:
            b = db.conn.execute("SELECT status FROM bp_request WHERE bp_request_id=?",(jid,)).fetchone()
            if not b or action!="cancel" or b[0] not in ("pending","expanded","running"):
                raise ValueError("Действие недоступно.")
            for sr in db.get_stage_requests_by_bp_request(jid):
                run = db.get_run_by_stage_request(sr["stage_request_id"])
                if run and run["status"]=="running":
                    Release(db).release_run(run["run_id"],"cancelled")
                elif not run:
                    db.update_stage_request_status(sr["stage_request_id"],"cancelled")
            db.conn.execute("UPDATE bp_request SET status='cancelled' WHERE bp_request_id=?",(jid,))
            db.log_event("run_cancelled","bp_request",jid,{"message":"Остановлен оператором"})

    def execution_error(self,jid,token,error):
        with self.tx() as db:
            if self._owned_run(db,jid,token):
                Release(db).release_run(int(token),"failed")
                db.log_event("run_failed","bp_request",jid,{"message":"Ошибка исполнителя: "+str(error)})
