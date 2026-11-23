import sqlite3
import json
from datetime import datetime
from pathlib import Path

class OrchestratorDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path, isolation_level=None) # Autocommit для явного управления
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        # Включаем критичные настройки SQLite
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA foreign_keys=ON;")
        self.conn.execute("PRAGMA busy_timeout=5000;") # Ждать 5 сек при блокировке файла
        
        # Создаём таблицы из schema.sql
        schema_path = Path(__file__).parent / "schema.sql"
        self.conn.executescript(schema_path.read_text())

    # --- Бизнес-процессы и этапы (Справочники) ---
    def create_business_process(self, name: str, risk_type: str, route: dict) -> int:
        cursor = self.conn.execute(
            "INSERT INTO business_process (name, risk_type, route, created_at) VALUES (?, ?, ?, ?)",
            (name, risk_type, json.dumps(route), datetime.now().isoformat())
        )
        return cursor.lastrowid

    # --- Заявки и Дедупликация ---
    def create_bp_request(self, bp_id: int, date: str, initiator_type: str, initiator_id: str, payload: dict) -> dict:
        """Создаёт заявку. Возвращает статус и ID (или ID существующего дубликата)."""
        input_hash = self._calculate_hash(bp_id, date, payload)
        
        try:
            cursor = self.conn.execute(
                """INSERT INTO bp_request 
                   (business_process_id, date, initiator_type, initiator_id, input_payload, input_hash, submitted_at, status) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')""",
                (bp_id, date, initiator_type, initiator_id, json.dumps(payload), input_hash, datetime.now().isoformat())
            )
            return {"status": "created", "id": cursor.lastrowid, "merged": False}
        except sqlite3.IntegrityError:
            # Сработал уникальный индекс -> это дубликат
            existing = self.conn.execute(
                "SELECT bp_request_id, status FROM bp_request WHERE business_process_id=? AND date=? AND input_hash=?",
                (bp_id, date, input_hash)
            ).fetchone()
            return {"status": "merged", "id": existing["bp_request_id"], "merged": True}

    # --- Блокировки (Атомарный захват) ---
    def acquire_locks(self, run_id: int, locks: list[dict]) -> bool:
        """Пытается захватить список блокировок в одной транзакции. Возвращает True при успехе."""
        # locks: [{'resource_id': 'X', 'data_key': 'Y', 'mode': 'write', 'granularity': 'partition'}]
        try:
            self.conn.execute("BEGIN IMMEDIATE;") # Блокируем БД на запись для проверки
            for lock in locks:
                # Проверка на конфликт (упрощённо: ищем несовместимые режимы в состоянии held)
                conflict = self.conn.execute(
                    """SELECT 1 FROM lock 
                       WHERE resource_id=? AND data_key=? AND state='held' 
                       AND NOT (mode='read' AND ?='read')""", # read+read можно
                    (lock['resource_id'], lock['data_key'], lock['mode'])
                ).fetchone()
                
                if conflict:
                    self.conn.execute("ROLLBACK;")
                    return False # Ресурс занят

                # Вставляем новую блокировку
                self.conn.execute(
                    """INSERT INTO lock (resource_id, granularity, data_key, run_id, mode, acquired_at, expires_at, state)
                       VALUES (?, ?, ?, ?, ?, ?, datetime('now', '+30 minutes'), 'held')""",
                    (lock['resource_id'], lock['granularity'], lock['data_key'], run_id, lock['mode'], datetime.now().isoformat())
                )
            self.conn.execute("COMMIT;")
            return True
        except Exception:
            self.conn.execute("ROLLBACK;")
            return False

    # --- Журнал событий ---
    def log_event(self, event_type: str, entity_type: str, entity_id: int, payload: dict = None, actor: str = "system"):
        self.conn.execute(
            """INSERT INTO event (timestamp, event_type, entity_type, entity_id, payload, actor)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (datetime.now().isoformat(), event_type, entity_type, entity_id, json.dumps(payload) if payload else None, actor)
        )

    # --- Утилиты ---
    def _calculate_hash(self, bp_id: int, date: str, payload: dict) -> str:
        import hashlib
        # Нормализация: сортировка ключей
        normalized = json.dumps(payload, sort_keys=True, separators=(',', ':'))
        raw = f"{bp_id}{date}{normalized}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def close(self):
        self.conn.close()