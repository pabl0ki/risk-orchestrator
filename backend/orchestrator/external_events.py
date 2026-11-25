"""Модуль приёма внешних событий (согласование, утверждение)."""
from backend.database.connection import OrchestratorDB
from backend.orchestrator.release import Release


class ExternalEvents:
    """Принимает внешние сигналы и обновляет запуски."""

    def __init__(self, db: OrchestratorDB):
        self.db = db
        self.release = Release(db)

    def receive_signal(self, run_id: int, actor: str, decision: str, comment: str = "") -> dict:
        """
        Принимает сигнал согласования/утверждения.
        
        decision: 'approve' или 'reject'
        """
        if decision not in ("approve", "reject"):
            return {"error": "Некорректное решение"}
        
        # 1. Проверяем запуск
        run = self.db.conn.execute(
            "SELECT * FROM run WHERE run_id = ?", (run_id,)
        ).fetchone()
        
        if not run:
            return {"error": "Запуск не найден"}
        
        if run["status"] != "waiting_external":
            return {"error": f"Запуск не в статусе waiting_external (текущий: {run['status']})"}
        
        # 2. Обновляем external_event
        external_event = {
            "actor": actor,
            "decision": decision,
            "comment": comment,
            "received_at": "now"
        }
        self.db.update_run_external_event(run_id, external_event)
        
        # 3. Логируем событие
        self.db.log_event(
            "external_signal", "run", run_id,
            external_event, actor
        )
        
        # 4. Обрабатываем решение
        if decision == "approve":
            # Возвращаем в running для финального коммита
            self.db.update_run_status(run_id, "running")
            self.db.log_event(
                "run_resumed", "run", run_id,
                {"reason": "Согласование получено"}, actor
            )
            return {"run_id": run_id, "status": "running", "action": "resume"}
        else:  # reject
            # Завершаем запуск
            result = self.release.release_run(run_id, "failed")
            return {"run_id": run_id, "status": "failed", "action": "reject", **result}