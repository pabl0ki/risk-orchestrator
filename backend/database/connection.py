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

    # --- Заявки и дедупликация ---

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

    # --- Закрытие ---

    def close(self):
        self.conn.close()