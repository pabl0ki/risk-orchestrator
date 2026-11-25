"""Оркестратор на реляционной схеме. API для фронта сохранён."""
import hashlib
import json
import time
import uuid
from concurrent.futures import Future
from contextlib import closing, contextmanager
from datetime import date
from threading import Lock

from backend.database.connection import OrchestratorDB

# --- Каталог процессов (маппинг на бизнес-процессы) ---
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

LEASE_SECONDS = 60
HEARTBEAT_INTERVAL = 10
STALE_THRESHOLD = 30
SLOTS = 4


def now():
    return time.time()

class Engine:
    """Оркестратор на реляционной схеме."""

    def __init__(self, db_path):
        self.db_path = str(db_path)
        self.inbox_lock = Lock()
        self.inbox = []
        self._init_catalog()

    def _init_catalog(self):
        """Инициализирует справочные данные (business_process, stage) из CATALOG."""
        with closing(OrchestratorDB(self.db_path)) as db:
            for kind, (name, res_list) in CATALOG.items():
                # Проверяем, есть ли уже бизнес-процесс
                existing = db.get_business_process_by_name(name)
                if not existing:
                    bp_id = db.create_business_process(name, kind, {"version": 1})
                    # Создаём этап для каждого процесса
                    stage_resources = json.dumps([
                        {"resource_id": r[0], "mode": r[1], "granularity": "partition"}
                        for r in res_list
                    ])
                    db.create_stage(bp_id, "calculation", stage_resources, None, 4, 10)

    @contextmanager
    def tx(self):
        """Контекстный менеджер транзакции (совместимость со старым API)."""
        db = OrchestratorDB(self.db_path)
        try:
            yield db
        finally:
            db.close()

    def snapshot(self):
        """Возвращает состояние в формате, совместимом со старым API."""
        with closing(OrchestratorDB(self.db_path)) as db:
            # Собираем jobs из run + stage_request + bp_request
            runs = db.get_all_runs_with_context()
            jobs = []
            for run in runs:
                job = {
                    "id": run["run_id"],
                    "kind": run["bp_name"],  # маппинг: bp_name → kind
                    "day": run["date"],
                    "factor": 1,  # для MVP
                    "resources": json.loads(run["required_resources"]),
                    "parent": run["parent_run_id"] or 0,
                    "route": 1,
                    "approval": run["stage_type"] in ("approval", "sign-off"),
                    "channel": run["initiator_type"],
                    "actor": run["initiator_id"],
                    "status": self._map_run_status(run["run_status"]),
                    "created": run["created_at"],
                    "start": run["started_at"],
                    "end": run["finished_at"],
                    "attempt": 1,
                    "token": None,  # в новой модели token не используется
                    "lease": run["lease_expires_at"],
                    "inputs": {},  # заполняется при claim
                    "result": run.get("result_payload"),
                    "reason": run.get("waiting_for", ""),
                    "rerun": False,
                    "wait_for": None,
                }
                jobs.append(job)

            # Собираем requests из bp_request
            bp_requests = db.get_all_bp_requests()
            requests = [
                {
                    "key": br["input_hash"],
                    "signature": [br["business_process_id"], br["date"], 1, br["initiator_id"], br["initiator_type"], False, False, 0, False],
                    "job": br["bp_request_id"],
                    "actor": br["initiator_id"],
                    "channel": br["initiator_type"],
                }
                for br in bp_requests
            ]

            # Собираем events
            events = db.get_recent_events(1000)
            events_formatted = [
                {
                    "seq": e["event_id"],
                    "at": e["timestamp"],
                    "job": e["entity_id"],
                    "message": e["payload"] if e["payload"] else e["event_type"],
                }
                for e in events
            ]

            return {
                "jobs": jobs,
                "requests": requests,
                "results": {},  # в новой модели результаты хранятся в run
                "heads": {},  # в новой модели публикации отслеживаются через run
                "events": events_formatted,
                "signals": {},  # внешние события хранятся в run.external_event
                "demo_batches": [],  # для MVP не используем
                "schedule": None,  # для MVP не используем
            }

    def _map_run_status(self, run_status):
        """Маппинг статусов run на статусы старого API."""
        mapping = {
            "queued": "queued",
            "waiting_resource": "queued",
            "running": "running",
            "waiting_external": "review",
            "done": "done",
            "failed": "failed",
            "cancelled": "cancelled",
        }
        return mapping.get(run_status, "queued")

    def submit(self, data):
        """Создаёт заявку на бизнес-процесс."""
        answer = Future()
        with self.inbox_lock:
            first = not self.inbox
            self.inbox.append((data, answer))
        if first:
            time.sleep(0.015)
            with self.inbox_lock:
                batch, self.inbox = self.inbox, []
            outcomes = []
            try:
                with closing(OrchestratorDB(self.db_path)) as db:
                    for request, pending in batch:
                        try:
                            jid = self._add(db, request)
                            outcomes.append((pending, jid, None))
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

    def _add(self, db, data):
        """Внутренний метод создания заявки."""
        kind = data["kind"]
        day = date.fromisoformat(data["day"]).isoformat()
        factor = int(data.get("factor", 1))
        actor = str(data.get("actor", "")).strip()
        channel = data.get("channel", "manual")

        if kind not in CATALOG:
            raise ValueError("Процесс не найден.")
        if not actor or len(actor) > 80:
            raise ValueError("Некорректный инициатор.")

        # Находим business_process
        bp = db.get_business_process_by_name(CATALOG[kind][0])
        if not bp:
            raise ValueError("Бизнес-процесс не найден.")

        # Создаём bp_request
        payload = {"factor": factor, "shared": bool(data.get("shared")), "rerun": bool(data.get("rerun"))}
        result = db.create_bp_request(
            bp["business_process_id"], day, channel, actor, payload
        )

        # Разворачиваем в stage_request
        from backend.orchestrator.expand import Expand
        expand = Expand(db)
        expand.expand_bp_request(result["id"])

        return result["id"]

    def claim(self):
        """Выбирает заявки для выполнения."""
        with closing(OrchestratorDB(self.db_path)) as db:
            # Находим stage_request в статусе created/queued
            pending = db.get_pending_stage_requests(100)
            started = []

            for sr in pending:
                # Проверяем зависимости
                all_done, unfinished = db.check_dependencies(sr["stage_request_id"])
                if not all_done:
                    db.update_stage_request_status(
                        sr["stage_request_id"], "waiting_dependency",
                        {"type": "dependency", "ids": unfinished}
                    )
                    continue

                # Проверяем ресурсы
                stage = db.get_stage(sr["stage_id"])
                required_resources = json.loads(stage["required_resources"] or "[]")
                
                # Проверяем конфликты
                conflicts = []
                for resource in required_resources:
                    active_locks = db.get_active_locks(resource["resource_id"], sr["date"])
                    for lock in active_locks:
                        if not (lock["mode"] == "read" and resource["mode"] == "read"):
                            conflicts.append(lock)

                if conflicts:
                    db.update_stage_request_status(
                        sr["stage_request_id"], "waiting_resource",
                        {"type": "resource", "conflicts": conflicts}
                    )
                    continue

                # Захватываем ресурсы
                locks = [
                    {
                        "resource_id": r["resource_id"],
                        "granularity": r.get("granularity", "partition"),
                        "data_key": sr["date"],
                        "mode": r["mode"],
                    }
                    for r in required_resources
                ]

                if not db.acquire_locks(sr["stage_request_id"], locks):
                    continue

                # Создаём run
                run_id = db.create_run(sr["stage_request_id"])
                db.update_run_status(run_id, "running", started_at=time.time())
                db.update_heartbeat(run_id)

                # Собираем job для worker
                bp = db.get_business_process(stage["business_process_id"])
                job = {
                    "id": run_id,
                    "kind": bp["risk_type"],
                    "day": sr["date"],
                    "factor": 1,
                    "resources": [[r["resource_id"], sr["date"], r["mode"]] for r in required_resources],
                    "parent": 0,
                    "route": 1,
                    "approval": stage["stage_type"] in ("approval", "sign-off"),
                    "channel": "manual",
                    "actor": "system",
                    "status": "running",
                    "created": sr["submitted_at"],
                    "start": time.time(),
                    "end": None,
                    "attempt": 1,
                    "token": str(run_id),
                    "lease": time.time() + LEASE_SECONDS,
                    "inputs": read_inputs(db, {"resources": [[r["resource_id"], sr["date"], r["mode"]] for r in required_resources]}),
                    "result": None,
                    "reason": "",
                    "rerun": False,
                    "wait_for": None,
                }
                started.append(job)

            return started

    def heartbeat(self, jid, token):
        """Обновляет heartbeat для запуска."""
        with closing(OrchestratorDB(self.db_path)) as db:
            return db.update_heartbeat(jid)

    def publish(self, run_id: int, token: str, result: dict) -> str:
        """Фиксирует факт завершения запуска."""
        with closing(OrchestratorDB(self.db_path)) as db:
            run = db.get_run_by_id(run_id)
            if not run or run["status"] != "running":
                return None
            
            db.update_run_status(run_id, "done", finished_at=time.time())
            db.release_locks(run_id)
            db.log_event("run_done", "run", run_id, 
                        {"rows_count": result.get("rows_count", 0)}, "system")
            
            # Будим детей
            sr = db.get_run_by_stage_request(run_id)
            if sr:
                db.wake_children(sr["stage_request_id"])
            
            return str(run_id)

    def decide(self, jid, decision, actor, event_id, comment="", version=None):
        """Обрабатывает решение по согласованию."""
        if decision not in ("approve", "reject"):
            raise ValueError("Некорректное решение.")

        with closing(OrchestratorDB(self.db_path)) as db:
            run = db.conn.execute(
                "SELECT * FROM run WHERE run_id = ?", (jid,)
            ).fetchone()

            if not run:
                raise ValueError("Запуск не найден.")

            if run["status"] != "waiting_external":
                raise ValueError("Запуск не в статусе ожидания согласования.")

            # Обновляем external_event
            external_event = {
                "actor": actor,
                "decision": decision,
                "comment": comment,
                "received_at": time.time(),
            }
            db.update_run_external_event(jid, external_event)

            if decision == "approve":
                db.update_run_status(jid, "done", finished_at=time.time())
            else:
                db.update_run_status(jid, "failed", finished_at=time.time())

            db.release_locks(jid)
            db.log_event("external_signal", "run", jid, external_event, actor)

            return None

    def control(self, jid, action):
        """Управляет запуском (отмена, сбой)."""
        with closing(OrchestratorDB(self.db_path)) as db:
            run = db.conn.execute(
                "SELECT * FROM run WHERE run_id = ?", (jid,)
            ).fetchone()

            if not run:
                raise ValueError("Запуск не найден.")

            if action == "cancel" and run["status"] in ("queued", "running"):
                db.update_run_status(jid, "cancelled", finished_at=time.time())
                db.release_locks(jid)
                db.log_event("run_cancelled", "run", jid, {"reason": "Остановлен оператором"}, "system")
            elif action == "fault" and run["status"] == "running":
                db.update_run_status(jid, "failed", finished_at=time.time())
                db.release_locks(jid)
                db.log_event("run_failed", "run", jid, {"reason": "Демонстрационный сбой"}, "system")
            else:
                raise ValueError("Действие недоступно в текущем состоянии.")

    def execution_error(self, jid, token, error):
        """Обрабатывает ошибку исполнения."""
        with closing(OrchestratorDB(self.db_path)) as db:
            db.update_run_status(jid, "failed", finished_at=time.time())
            db.release_locks(jid)
            db.log_event("run_failed", "run", jid, {"error": str(error)}, "system")