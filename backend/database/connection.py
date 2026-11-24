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
        """Возвращает бизнес-процесс по ID или None, если не найден."""
        return self.conn.execute(
            queries.GET_BUSINESS_PROCESS, (bp_id,)
        ).fetchone()

    def get_stages(self, bp_id: int) -> list[dict]:
        """Возвращает все этапы бизнес-процесса."""
        return self.conn.execute(
            queries.GET_STAGES_BY_BP_ID, (bp_id,)
        ).fetchall()

    # --- Заявки на бизнес-процессы ---

    def create_bp_request(
        self,
        bp_id: int,
        date: str,
        initiator_type: str,
        initiator_id: str,
        payload: dict,
    ) -> dict:
        """Создаёт заявку на бизнес-процесс. Возвращает {"status", "id", "merged"}."""
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
        """Создаёт заявку на этап. Возвращает stage_request_id."""
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
        """Возвращает все заявки на этапы для конкретного bp_request."""
        return self.conn.execute(
            queries.GET_STAGE_REQUESTS_BY_BP_REQUEST, (bp_request_id,)
        ).fetchall()

    def update_stage_request_status(
        self, stage_request_id: int, status: str, waiting_for: str | None = None
    ):
        """Обновляет статус заявки на этап."""
        self.conn.execute(
            queries.UPDATE_STAGE_REQUEST_STATUS,
            (status, json.dumps(waiting_for) if waiting_for else None, stage_request_id),
        )

    # --- Запуски ---

    def create_run(self, stage_request_id: int) -> int:
        """Создаёт запуск для одобренной заявки на этап."""
        cursor = self.conn.execute(
            queries.INSERT_RUN,
            (stage_request_id, datetime.now().isoformat()),
        )
        return cursor.lastrowid

    def update_run_status(
        self, run_id: int, status: str, started_at: str | None = None, finished_at: str | None = None
    ):
        """Обновляет статус запуска и временные метки."""
        self.conn.execute(
            queries.UPDATE_RUN_STATUS,
            (status, started_at, started_at, finished_at, finished_at, run_id),
        )

    # --- Блокировки ---

    def acquire_locks(self, run_id: int, locks: list[dict]) -> bool:
        """Атомарный захват списка блокировок. Возвращает True при успехе."""
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


        # --- Выборки для admit/sweep ---

    def get_pending_stage_requests(self, limit: int = 100) -> list[dict]:
        """Заявки в статусах created/queued для обработки в admit."""
        return self.conn.execute(
            queries.GET_PENDING_STAGE_REQUESTS, (limit,)
        ).fetchall()

    def get_stage_requests_by_parent(self, parent_id: int) -> list[dict]:
        """Дочерние заявки, у которых parent_id в списке родителей."""
        return self.conn.execute(
            queries.GET_STAGE_REQUESTS_BY_PARENT, (f"%{parent_id}%",)
        ).fetchall()

    def get_run_by_stage_request(self, stage_request_id: int) -> dict | None:
        """Запуск по stage_request_id."""
        return self.conn.execute(
            queries.GET_RUN_BY_STAGE_REQUEST, (stage_request_id,)
        ).fetchone()

    def get_runs_by_status(self, status: str) -> list[dict]:
        """Все запуски в заданном статусе."""
        return self.conn.execute(
            queries.GET_RUNS_BY_STATUS, (status,)
        ).fetchall()

    # --- Блокировки: освобождение и выборки ---

    def release_locks(self, run_id: int) -> int:
        """Освобождает все held-блокировки запуска. Возвращает число освобождённых."""
        cursor = self.conn.execute(
            queries.UPDATE_LOCK_STATE,
            ("released", datetime.now().isoformat(), run_id),
        )
        return cursor.rowcount

    def get_locks_by_run(self, run_id: int) -> list[dict]:
        """Все блокировки запуска."""
        return self.conn.execute(
            queries.GET_LOCKS_BY_RUN, (run_id,)
        ).fetchall()

    def get_active_locks(self, resource_id: str, data_key: str) -> list[dict]:
        """Активные (held) блокировки ресурса."""
        return self.conn.execute(
            queries.GET_ACTIVE_LOCKS, (resource_id, data_key)
        ).fetchall()

    def get_stages_by_ids(self, stage_ids: list[int]) -> list[dict]:
        """Этапы по списку ID (для DAG-валидации)."""
        if not stage_ids:
            return []
        placeholders = ",".join("?" * len(stage_ids))
        query = queries.GET_STAGES_BY_IDS.format(placeholders)
        return self.conn.execute(query, stage_ids).fetchall()

    # --- Закрытие ---

    def close(self):
        self.conn.close()