"""Модуль освобождения ресурсов после завершения запуска."""
from backend.database.connection import OrchestratorDB


class Release:
    """Освобождает блокировки и будит дочерние заявки."""

    def __init__(self, db: OrchestratorDB):
        self.db = db

    def release_run(self, run_id: int, status: str) -> dict:
        """
        Завершает запуск и освобождает ресурсы.
        
        status: 'done', 'failed', 'cancelled'
        """
        # 1. Завершаем запуск
        self.db.finish_run(run_id, status)
        
        # 2. Освобождаем блокировки
        released_count = self.db.release_locks(run_id)
        
        # 3. Получаем stage_request
        run = self.db.conn.execute(
            "SELECT stage_request_id FROM run WHERE run_id = ?", (run_id,)
        ).fetchone()
        
        if not run:
            return {"run_id": run_id, "status": status, "released_locks": 0, "woken_children": 0}
        
        sr_id = run["stage_request_id"]
        
        # 4. Будим детей (если статус done)
        woken_count = 0
        if status == "done":
            woken = self.db.wake_children(sr_id)
            woken_count = len(woken)
        elif status in ("failed", "cancelled"):
            # Дети переходят в rejected
            children = self.db.get_stage_requests_by_parent(sr_id)
            for child in children:
                if child["status"] in ("waiting_dependency", "created", "queued"):
                    self.db.update_stage_request_status(
                        child["stage_request_id"], "rejected",
                        {"type": "parent_failed", "parent_run_id": run_id}
                    )
                    self.db.log_event(
                        "stage_request_rejected", "stage_request", child["stage_request_id"],
                        {"reason": f"Родитель {run_id} завершился со статусом {status}"},
                        "system"
                    )
        
        # 5. Логируем
        self.db.log_event(
            "run_released", "run", run_id,
            {"status": status, "released_locks": released_count, "woken_children": woken_count},
            "system"
        )
        
        return {
            "run_id": run_id,
            "status": status,
            "released_locks": released_count,
            "woken_children": woken_count
        }