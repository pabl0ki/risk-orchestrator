"""Слой подключения к БД и бизнес-методы доступа к данным."""
import sqlite3
import json
from datetime import datetime
from pathlib import Path

from backend.database.utils import calculate_hash
from backend.database import queries


class OrchestratorDB:
    """Единая точка доступа к SQLite-базе оркестратора."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    # --- Инициализация ---

    def _init_db(self):
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA busy_timeout=5000;")

        schema_path = Path(__file__).parent / "schema.sql"
        self.conn.executescript(schema_path.read_text())

    # --- Справочники ---

    def create_business_process(self, name: str, risk_type: str, route: dict) -> int:
        cursor = self.conn.execute(
            queries.INSERT_BUSINESS_PROCESS,
            (name, risk_type, json.dumps(route), datetime.now().isoformat()),
        )
        return cursor.lastrowid

    def get_business_process(self, bp_id: int) -> dict | None:
        return self.conn.execute(
            queries.GET_BUSINESS_PROCESS, (bp_id,)
        ).fetchone()

    def get_stages(self, bp_id: int) -> list[dict]:
        return self.conn.execute(
            queries.GET_STAGES_BY_BP_ID, (bp_id,)
        ).fetchall()

    def get_all_business_processes(self) -> list[dict]:
        return self.conn.execute(queries.GET_ALL_BUSINESS_PROCESSES).fetchall()

    def get_stage(self, stage_id: int) -> dict | None:
        return self.conn.execute(
            queries.GET_STAGE_BY_ID, (stage_id,)
        ).fetchone()

    def update_business_process(self, bp_id: int, name: str, risk_type: str, route: dict) -> None:
        self.conn.execute(
            queries.UPDATE_BUSINESS_PROCESS,
            (name, risk_type, json.dumps(route), bp_id),
        )

    # --- Заявки на бизнес-процессы ---

    def get_bp_request_by_hash(self, bp_id: int, date: str, input_hash: str) -> dict | None:
        return self.conn.execute(
            queries.GET_BP_REQUEST_BY_HASH,
            (bp_id, date, input_hash),
        ).fetchone()

    def create_bp_request(
        self,
        bp_id: int,
        date: str,
        initiator_type: str,
        initiator_id: str,
        payload: dict,
    ) -> dict:
        input_hash = calculate_hash(bp_id, date, payload)
        try:
            cursor = self.conn.execute(
                queries.INSERT_BP_REQUEST,
                (
                    bp_id,
                    date,
                    initiator_type,
                    initiator_id,
                    json.dumps(payload),
                    input_hash,
                    datetime.now().isoformat(),
                ),
            )
            return {"status": "created", "id": cursor.lastrowid, "merged": False}
        except sqlite3.IntegrityError:
            existing = self.conn.execute(
                queries.GET_BP_REQUEST_BY_HASH,
                (bp_id, date, input_hash),
            ).fetchone()
            return {"status": "merged", "id": existing["bp_request_id"], "merged": True}

    # --- Заявки на этапы ---

    def create_stage_request(
        self,
        bp_request_id: int,
        stage_id: int,
        date: str,
        priority: int,
        input_hash: str,
        parent_stage_request_ids: list[int] | None,
    ) -> int:
        status = "created" if not parent_stage_request_ids else "waiting_dependency"
        cursor = self.conn.execute(
            queries.INSERT_STAGE_REQUEST,
            (
                bp_request_id,
                stage_id,
                date,
                priority,
                input_hash,
                datetime.now().isoformat(),
                status,
                json.dumps(parent_stage_request_ids) if parent_stage_request_ids else None,
            ),
        )
        return cursor.lastrowid

    def get_stage_requests_by_bp_request(self, bp_request_id: int) -> list[dict]:
        return self.conn.execute(
            queries.GET_STAGE_REQUESTS_BY_BP_REQUEST, (bp_request_id,)
        ).fetchall()

    def update_stage_request_status(
        self, stage_request_id: int, status: str, waiting_for: str | None = None
    ):
        self.conn.execute(
            queries.UPDATE_STAGE_REQUEST_STATUS,
            (status, json.dumps(waiting_for) if waiting_for else None, stage_request_id),
        )

    def get_stage_requests_by_parent(self, parent_id: int) -> list[dict]:
        return self.conn.execute(
            queries.GET_STAGE_REQUESTS_BY_PARENT, (parent_id,)
        ).fetchall()

    # --- Запуски ---

    def create_run(self, stage_request_id: int) -> int:
        cursor = self.conn.execute(
            queries.INSERT_RUN, (stage_request_id, datetime.now().isoformat()),
        )
        return cursor.lastrowid

    def update_run_status(
        self, run_id: int, status: str, started_at: str | None = None, finished_at: str | None = None
    ):
        self.conn.execute(
            queries.UPDATE_RUN_STATUS,
            (status, started_at, started_at, finished_at, finished_at, run_id),
        )

    def get_run_by_stage_request(self, stage_request_id: int) -> dict | None:
        return self.conn.execute(
            queries.GET_RUN_BY_STAGE_REQUEST, (stage_request_id,)
        ).fetchone()

    def get_runs_by_status(self, status: str) -> list[dict]:
        return self.conn.execute(
            queries.GET_RUNS_BY_STATUS, (status,)
        ).fetchall()

    def update_heartbeat(self, run_id: int) -> bool:
        cursor = self.conn.execute(queries.UPDATE_RUN_HEARTBEAT, (run_id,))
        return cursor.rowcount > 0

    def get_stale_runs(self, threshold_seconds: int) -> list[dict]:
        return self.conn.execute(
            queries.GET_STALE_RUNS, (f"-{threshold_seconds} seconds",)
        ).fetchall()

    # --- Блокировки ---

    def acquire_locks(self, run_id: int, locks: list[dict]) -> bool:
        try:
            self.conn.execute("BEGIN IMMEDIATE;")
            for lock in locks:
                conflict = self.conn.execute(
                    queries.CHECK_LOCK_CONFLICT,
                    (lock["resource_id"], lock["data_key"], lock["mode"]),
                ).fetchone()
                if conflict:
                    self.conn.execute("ROLLBACK;")
                    return False
                self.conn.execute(
                    queries.INSERT_LOCK,
                    (
                        lock["resource_id"],
                        lock["granularity"],
                        lock["data_key"],
                        run_id,
                        lock["mode"],
                        datetime.now().isoformat(),
                    ),
                )
            self.conn.execute("COMMIT;")
            return True
        except Exception:
            self.conn.execute("ROLLBACK;")
            return False

    def release_locks(self, run_id: int) -> int:
        cursor = self.conn.execute(
            queries.UPDATE_LOCK_STATE,
            ("released", datetime.now().isoformat(), run_id),
        )
        return cursor.rowcount

    def get_locks_by_run(self, run_id: int) -> list[dict]:
        return self.conn.execute(queries.GET_LOCKS_BY_RUN, (run_id,)).fetchall()

    def get_active_locks(self, resource_id: str, data_key: str) -> list[dict]:
        return self.conn.execute(
            queries.GET_ACTIVE_LOCKS, (resource_id, data_key)
        ).fetchall()

    # --- Журнал событий ---

    def log_event(
        self,
        event_type: str,
        entity_type: str,
        entity_id: int,
        payload: dict = None,
        actor: str = "system",
    ):
        self.conn.execute(
            queries.INSERT_EVENT,
            (
                datetime.now().isoformat(),
                event_type,
                entity_type,
                entity_id,
                json.dumps(payload) if payload else None,
                actor,
            ),
        )

    def get_events_by_entity(self, entity_type: str, entity_id: int) -> list[dict]:
        return self.conn.execute(
            queries.GET_EVENTS_BY_ENTITY, (entity_type, entity_id)
        ).fetchall()

    def get_events_by_type(self, event_type: str) -> list[dict]:
        return self.conn.execute(queries.GET_EVENTS_BY_TYPE, (event_type,)).fetchall()

    def get_recent_events(self, limit: int = 100) -> list[dict]:
        return self.conn.execute(queries.GET_RECENT_EVENTS, (limit,)).fetchall()

    # --- Проверка зависимостей (для admit) ---

    def get_stage_request_parents(self, stage_request_id: int) -> list[int]:
        row = self.conn.execute(
            queries.GET_STAGE_REQUEST_BY_ID, (stage_request_id,)
        ).fetchone()
        if not row or not row["parent_stage_request_ids"]:
            return []
        return json.loads(row["parent_stage_request_ids"])

    def check_dependencies(self, stage_request_id: int) -> tuple[bool, list[int]]:
        parent_ids = self.get_stage_request_parents(stage_request_id)
        if not parent_ids:
            return True, []
        placeholders = ",".join("?" * len(parent_ids))
        query = f"""
            SELECT sr.stage_request_id, sr.status, r.status AS run_status
            FROM stage_request sr
            LEFT JOIN run r ON r.stage_request_id = sr.stage_request_id
            WHERE sr.stage_request_id IN ({placeholders})
        """
        parents = self.conn.execute(query, parent_ids).fetchall()
        unfinished = [
            p["stage_request_id"]
            for p in parents
            if p["run_status"] != "done"
        ]
        return len(unfinished) == 0, unfinished

    # --- Завершение и пробуждение (для release) ---

    def finish_run(self, run_id: int, status: str) -> None:
        self.conn.execute(
            queries.UPDATE_RUN_FINISHED,
            (status, datetime.now().isoformat(), run_id),
        )

    def wake_children(self, parent_stage_request_id: int) -> list[int]:
        children = self.get_stage_requests_by_parent(parent_stage_request_id)
        woken_ids = []
        for child in children:
            if child["status"] not in ("waiting_dependency", "created"):
                continue
            all_done, _ = self.check_dependencies(child["stage_request_id"])
            if all_done:
                self.update_stage_request_status(child["stage_request_id"], "queued")
                woken_ids.append(child["stage_request_id"])
        return woken_ids

    # --- Фоновый обход и таймауты (для sweep_queue) ---

    def get_all_queued_requests(self) -> list[dict]:
        return self.conn.execute(queries.GET_ALL_QUEUED_REQUESTS).fetchall()

    def get_expired_locks(self) -> list[dict]:
        return self.conn.execute(queries.GET_EXPIRED_LOCKS).fetchall()

    def expire_locks(self) -> int:
        cursor = self.conn.execute(queries.EXPIRE_LOCKS)
        return cursor.rowcount

    # --- Внешние события ---

    def update_run_external_event(self, run_id: int, external_event: dict) -> None:
        self.conn.execute(
            queries.UPDATE_RUN_EXTERNAL_EVENT, (json.dumps(external_event), run_id),
        )

    def get_runs_waiting_external(self) -> list[dict]:
        return self.conn.execute(queries.GET_RUNS_WAITING_EXTERNAL).fetchall()

    # --- DAG-валидация ---

    def get_stages_by_ids(self, stage_ids: list[int]) -> list[dict]:
        if not stage_ids:
            return []
        placeholders = ",".join("?" * len(stage_ids))
        query = queries.GET_STAGES_BY_IDS.format(placeholders)
        return self.conn.execute(query, stage_ids).fetchall()

    # --- UI / наблюдаемость ---

    def get_bp_request_full(self, bp_request_id: int) -> list[dict]:
        return self.conn.execute(queries.GET_BP_REQUEST_FULL, (bp_request_id,)).fetchall()

    def get_queue_snapshot(self) -> list[dict]:
        return self.conn.execute(queries.GET_QUEUE_SNAPSHOT).fetchall()

    def get_locks_snapshot(self) -> list[dict]:
        return self.conn.execute(queries.GET_LOCKS_SNAPSHOT).fetchall()

    # --- Закрытие ---

    def close(self):
        self.conn.close()
