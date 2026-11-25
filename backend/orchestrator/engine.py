"""Одноузловой демонстрационный оркестратор. Только стандартная библиотека."""
import hashlib
import json
import sqlite3
import time
import uuid
from concurrent.futures import Future
from contextlib import closing, contextmanager
from datetime import date
from threading import Lock

CATALOG = {
    "P1": ("Кредитный риск", [("deals", "R"), ("rates", "R"), ("credit", "W")]),
    "P2": ("Рыночный риск", [("deals", "R"), ("rates", "R"), ("market", "W")]),
    "Z1": ("Загрузка сделок", [("deals", "W"), ("rates", "R")]),
    "Z2": ("Загрузка курсов", [("rates", "W"), ("deals", "R")]),
    "FX": ("Обновление справочника", [("reference", "W")]),
}
LABELS = dict(deals="Сделки", rates="Курсы", credit="Кредитный результат",
              market="Рыночный результат", reference="Справочник", work="Общая рабочая таблица")
LEASE = 12
PROTECT = 3
SLOTS = 4


def now():
    return time.time()


def event(s, job, message):
    s["events"].append(dict(seq=len(s["events"]) + 1, at=now(), job=job, message=message))


def resources(kind, day, shared=False):
    specs = list(CATALOG[kind][1])
    if kind in ("P1", "P2"):
        specs.append(("reference", "R"))
    if shared:
        specs.append(("work", "W"))
    return [[name, "*" if name in ("reference", "work") else day, mode] for name, mode in specs]


def collision(left, right):
    for a, day, mode in left:
        for b, other, access in right:
            if a == b and (day == other or "*" in (day, other)) and "W" in (mode, access):
                return LABELS[a]
    return ""


def read_inputs(s, req):
    inputs = {}
    for name, day, mode in req["resources"]:
        if mode != "R":
            continue
        key = name + "/" + day
        version = s["heads"].get(key)
        if version:
            inputs[name] = s["results"][version]
        else:
            rows = []
            for i in range(8):
                digest = hashlib.sha256(f"{key}:{i}".encode()).digest()
                rows.append(10 + int.from_bytes(digest[:2], "big") % 80)
            inputs[name] = dict(id="base/" + key, rows=rows)
    return inputs


from .calculations import calculate


def output_key(job):
    name, scope, _ = next(resource for resource in job["resources"] if resource[2] == "W")
    return f"{name}/{scope}"


class Engine:
    def __init__(self, db):
        self.db = str(db)
        self.inbox_lock = Lock()
        self.inbox = []
        with self.tx() as s:
            if not s:
                s.update(jobs=[], requests=[], results={}, heads={}, events=[], schedule=None)
            s.setdefault("routes", {k: 1 for k in CATALOG})
            s.setdefault("signals", {})

    @contextmanager
    def tx(self):
        con = sqlite3.connect(self.db, timeout=20)
        try:
            con.execute("BEGIN IMMEDIATE")
            con.execute("CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)")
            row = con.execute("SELECT payload FROM ledger WHERE id=1").fetchone()
            state = json.loads(row[0]) if row else {}
            yield state
            payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
            if not row or row[0] != payload:
                con.execute("INSERT OR REPLACE INTO ledger VALUES (1,?)", (payload,))
            con.commit()
        finally:
            con.close()

    def snapshot(self):
        with closing(sqlite3.connect(self.db)) as con:
            return json.loads(con.execute("SELECT payload FROM ledger WHERE id=1").fetchone()[0])

    def add(self, s, data):
        kind, day = data["kind"], date.fromisoformat(data["day"]).isoformat()
        factor = int(data.get("factor", 1))
        actor = str(data.get("actor", "")).strip()
        channel = data.get("channel", "manual")
        if kind not in CATALOG or not actor or len(actor) > 80 or not 1 <= factor <= 10 or channel not in ("manual", "schedule"):
            raise ValueError("Проверьте тип процесса, инициатора и множитель (1–10).")
        dep = int(data.get("parent") or 0)
        if dep and not any(j["id"] == dep for j in s["jobs"]):
            raise ValueError("Предшественник не найден.")
        key = data.get("key") or uuid.uuid4().hex
        approval = bool(data.get("approval"))
        signature = [kind, day, factor, actor, channel, bool(data.get("shared")), bool(data.get("rerun")), dep, approval]
        for r in s["requests"]:
            if r["key"] == key:
                if r["signature"] != signature:
                    raise ValueError("Ключ обращения уже использован с другими параметрами.")
                return r["job"]
        locks = resources(kind, day, bool(data.get("shared")))
        cfg = dict(kind=kind, day=day, factor=factor, resources=locks, parent=dep,
                   route=s["routes"][kind], approval=approval)
        current = read_inputs(s, cfg)
        duplicate = None
        if not data.get("rerun"):
            for j in s["jobs"]:
                if j["status"] not in ("queued", "running") or j["rerun"]:
                    continue
                if all(j.get(k) == v for k, v in cfg.items()) and (not j["inputs"] or j["inputs"] == current):
                    duplicate = j
                    break
        if duplicate:
            jid = duplicate["id"]
        else:
            jid = len(s["jobs"]) + 1
            s["jobs"].append(dict(**cfg, id=jid, channel=channel, actor=actor, status="queued",
                created=now(), start=None, end=None, attempt=0, token=None, lease=0, inputs={},
                result=None, reason="Ожидает проверки", rerun=bool(data.get("rerun"))))
        s["requests"].append(dict(key=key, signature=signature, job=jid, actor=actor, channel=channel))
        event(s, jid, ("Присоединено обращение" if duplicate else "Принято обращение") + " · " + actor)
        return jid

    def submit(self, data):
        # Concurrent callers share a commit, including duplicate checks and event ordering.
        answer = Future()
        with self.inbox_lock:
            first = not self.inbox
            self.inbox.append((data, answer))
        if first:
            time.sleep(.015)
            with self.inbox_lock:
                batch, self.inbox = self.inbox, []
            outcomes = []
            try:
                with self.tx() as s:
                    for request, pending in batch:
                        try:
                            outcomes.append((pending, self.add(s, request), None))
                        except (ValueError, TypeError, KeyError) as err:
                            outcomes.append((pending, None, err))
            except Exception as err:
                outcomes = [(pending, None, err) for _, pending in batch]
            for pending, value, error in outcomes:
                if error is None:
                    pending.set_result(value)
                else:
                    pending.set_exception(error)
        return answer.result()

    def revise(self, kind):
        with self.tx() as s:
            if kind not in CATALOG:
                raise ValueError("Процесс не найден.")
            s["routes"][kind] += 1
            event(s, 0, "Зарегистрирована версия маршрута " + kind + " · " + str(s["routes"][kind]))

    def decide(self, jid, decision, actor, event_id, comment="", version=None):
        if decision not in ("approve", "reject") or not event_id:
            raise ValueError("Некорректное решение.")
        with self.tx() as s:
            prev = s["signals"].get(event_id)
            if prev:
                if prev[:3] != [jid, decision, actor] or prev[4:] != [comment, version]:
                    raise ValueError("ID решения уже использован.")
                return prev[3]
            if not 1 <= jid <= len(s["jobs"]):
                raise ValueError("Запуск не найден.")
            j = s["jobs"][jid-1]
            if j["status"] != "review" or any(r["actor"] == actor for r in s["requests"] if r["job"] == jid):
                raise ValueError("Согласование недоступно для данного пользователя или версии.")
            v = s["results"][j["result"]]
            expected = "Директор" if v["approval"] == "reviewed" else "Риск-менеджер"
            if actor != expected or (version is not None and version != v["id"]):
                raise ValueError("Решение не соответствует этапу или версии.")
            if decision == "reject" and not comment.strip():
                raise ValueError("Укажите причину отклонения.")
            if decision == "approve":
                checks = self.approval_checks(s, j, v)
                if checks["block_reason"]:
                    raise ValueError(checks["block_reason"])
            if decision == "approve" and actor == "Риск-менеджер":
                v["approval"] = "reviewed"
                j["reason"] = "Ожидает утверждения директора"
                event(s, jid, actor + " · согласовано " + v["id"] + " · " + comment)
                s["signals"][event_id] = [jid, decision, actor, None, comment, version]
                return None
            if decision == "approve":
                s["heads"][output_key(j)] = v["id"]
                self.invalidate_approvals(s)
            j.update(status="done" if decision=="approve" else "rejected", end=now(), reason="")
            v["approval"] = "approved" if decision=="approve" else "rejected"
            successor = None
            if decision == "reject":
                successor = self.add(s,dict(kind=j["kind"],day=j["day"],factor=j["factor"],actor=j["actor"],
                    shared=any(x[0]=="work" for x in j["resources"]),rerun=True,approval=True,parent=j["parent"]))
                for batch in s.get("demo_batches", []):
                    if jid in batch["jobs"]:
                        batch["jobs"].append(successor)
                        batch["requests"] += 1
                        break
            s["signals"][event_id]=[jid,decision,actor,successor,comment,version]
            event(s,jid,actor + (" · согласовано " if decision=="approve" else " · отклонено ") + v["id"] + " · " + comment)
            if successor:
                event(s, jid, f"Создан перерасчёт RUN-{successor} для версии {v['id']}")
                event(s, successor, f"Перерасчёт RUN-{jid} (версия {v['id']}) · {comment}")
            return successor

    def fresh(self, s, job, version):
        current = read_inputs(s,job)
        return all(current[k]["id"] == v for k,v in version["inputs"].items())

    def approval_checks(self, state, job, version):
        reasons = []
        inputs = read_inputs(state, job)
        for name, used in version["inputs"].items():
            latest = inputs[name]["id"]
            if used != latest:
                reasons.append(f"Изменился вход «{LABELS[name]}»: в расчёте — {used}, сейчас — {latest}.")
        previous = version.get("previous")
        published = state["heads"].get(output_key(job))
        if previous != published:
            reasons.append(f"Изменилась публикация «{LABELS[version['object']]}»: "
                           f"при расчёте — {previous or 'ещё не опубликовано'}, "
                           f"сейчас — {published or 'нет публикации'}.")
        current = not reasons
        verified = calculate(job) == version["rows"]
        if not verified:
            reasons.append("Построчная сверка выявила расхождение.")
        reason = " ".join([*reasons, "Требуется перерасчёт."]) if reasons else ""
        return dict(current=current, verified=verified, block_reason=reason)

    def schedule(self, data):
        with self.tx() as s:
            if not data.get("enabled"):
                s["schedule"] = None
            else:
                period = int(data["period"])
                if not 5 <= period <= 3600 or data["kind"] not in CATALOG:
                    raise ValueError("Интервал расписания — от 5 до 3600 секунд.")
                day = date.fromisoformat(data["day"]).isoformat()
                s["schedule"] = dict(kind=data["kind"], day=day, period=period, due=now()+period)
            event(s, 0, "Расписание изменено")

    def claim(self):
        started = []
        with self.tx() as s:
            t = now()
            plan = s["schedule"]
            if plan and plan["due"] <= t:
                self.add(s, dict(**plan, channel="schedule", actor="Регламент"))
                plan["due"] = t + plan["period"]
            for j in s["jobs"]:
                if j["status"] == "running" and j["lease"] <= t:
                    self.fail(s, j, "Истёк срок владения")
            active = [j for j in s["jobs"] if j["status"] == "running"]
            pending = [j for j in s["jobs"] if j["status"] == "queued"]
            pending.sort(key=lambda j: (0 if t-j["created"] >= PROTECT else 1,
                0 if t-j["created"] >= PROTECT or j["channel"]=="schedule" else 1, j["id"]))
            protected = []
            for j in pending:
                parent = s["jobs"][j["parent"]-1] if j["parent"] else None
                if parent and parent["status"] != "done":
                    if parent["status"] in ("failed", "cancelled", "rejected"):
                        j.update(status="rejected", end=t, reason="Предшественник завершился без результата")
                        event(s, j["id"], j["reason"])
                    else:
                        self.wait(s, j, "Ожидание предшественника №" + str(parent["id"]), parent["id"])
                    continue
                reason = ""
                blocker = None
                for owner in active + protected:
                    obj = collision(j["resources"], owner["resources"])
                    if obj:
                        reason = f"{obj} · ожидает №{owner['id']}"
                        blocker = owner["id"]
                        break
                if reason or len(active) >= SLOTS:
                    self.wait(s, j, reason or "Все исполнители заняты", blocker)
                    if t-j["created"] >= PROTECT:
                        protected.append(j)
                    continue
                inputs = j["inputs"] or read_inputs(s, j)
                if parent:
                    pv = s["results"][parent["result"]]
                    parent_scope = output_key(parent)
                    if any(mode == "R" and f"{name}/{scope}" == parent_scope
                           for name, scope, mode in j["resources"]):
                        inputs[pv["object"]] = pv
                j.update(status="running", start=t, token=uuid.uuid4().hex, lease=t+LEASE,
                         inputs=inputs, attempt=j["attempt"]+1, reason="", wait_for=None)
                event(s, j["id"], f"Допуск · попытка {j['attempt']} · получены все ресурсы")
                active.append(j)
                started.append(j.copy())
        return started

    def wait(self, s, job, reason, blocker=None):
        job["wait_for"] = blocker
        if job["reason"] != reason:
            job["reason"] = reason
            event(s, job["id"], reason)

    def fail(self, s, j, reason):
        retry = j["attempt"] < 2
        j.update(status="queued" if retry else "failed", token=None, lease=0,
                 reason=reason, end=None if retry else now())
        event(s, j["id"], reason + (" · назначен повтор" if retry else " · исчерпаны попытки"))

    def execution_error(self, jid, token, error):
        with self.tx() as s:
            job = s["jobs"][jid-1]
            if job["status"] == "running" and job["token"] == token and job["lease"] > now():
                self.fail(s, job, f"Ошибка исполнения: {type(error).__name__}: {error}")

    def heartbeat(self, jid, token):
        with self.tx() as s:
            j = s["jobs"][jid-1]
            if j["status"] == "running" and j["token"] == token and j["lease"] > now():
                j["lease"] = now() + LEASE
                return True
            return False

    def publish(self, jid, token, rows):
        with self.tx() as s:
            j = s["jobs"][jid-1]
            rid = f"v{jid}.{j['attempt']}"
            if rid in s["results"] and s["results"][rid]["token"] == token:
                return rid
            if j["status"] != "running" or j["token"] != token or j["lease"] <= now():
                return None
            key = output_key(j)
            target, scope = key.split("/", 1)
            s["results"][rid] = dict(id=rid, rows=rows, object=target, token=token,
                scope=scope, previous=s["heads"].get(key),
                inputs={k:v["id"] for k,v in j["inputs"].items()}, algorithm="demo-1", factor=j["factor"])
            review = j.get("approval",False)
            if not review:
                s["heads"][key] = rid
            s["results"][rid]["approval"] = "pending" if review else "unused"
            j.update(status="review" if review else "done", end=now(), calculated_at=now(),
                     result=rid, token=None, reason="")
            event(s, jid, ("Передано на согласование " if review else "Опубликовано ") + rid + " · ресурсы освобождены")
            if not review:
                self.invalidate_approvals(s)
            return rid

    def invalidate_approvals(self, s):
        for item in s["jobs"]:
            if not item.get("approval") or not item.get("result"):
                continue
            v = s["results"][item["result"]]
            if v["approval"] == "approved" and not self.fresh(s,item,v):
                v["approval"] = "stale"
                event(s,item["id"],"Согласование устарело · изменён вход версии " + v["id"])

    def control(self, jid, action):
        with self.tx() as s:
            if not 1 <= jid <= len(s["jobs"]):
                raise ValueError("Запуск не найден.")
            j = s["jobs"][jid-1]
            if action == "fault" and j["status"] == "running":
                self.fail(s, j, "Демонстрационный сбой")
            elif action == "cancel" and j["status"] in ("queued","running"):
                j.update(status="cancelled", token=None, end=now(), reason="Остановлен оператором")
                event(s, jid, j["reason"])
            else:
                raise ValueError("Действие недоступно в текущем состоянии.")

