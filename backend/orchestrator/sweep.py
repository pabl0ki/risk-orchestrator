"""Модуль периодического обхода очереди и очистки истекших блокировок."""
from backend.database.connection import OrchestratorDB
from backend.orchestrator.admit import Admit


class Sweep:
    """Периодический обход очереди и обслуживание системы."""

    def __init__(self, db: OrchestratorDB):
        self.db = db
        self.admit = Admit(db)

    def sweep_all(self) -> dict:
        """
        Выполняет полный обход:
        1. Чистит истекшие блокировки
        2. Пересматривает все заявки в очереди
        """
        # 1. Чистим истекшие блокировки
        expired_locks = self.db.get_expired_locks()
        expired_count = self.db.expire_locks()
        
        for lock in expired_locks:
            self.db.log_event(
                "lock_expired", "lock", lock["lock_id"],
                {"run_id": lock["run_id"], "resource_id": lock["resource_id"]},
                "system"
            )
        
        # 2. Пересматриваем все заявки в очереди
        queued = self.db.get_all_queued_requests()
        decisions = []
        
        for sr in queued:
            # Если заявка в waiting_resource — проверяем снова
            if sr["status"] == "waiting_resource":
                decision = self.admit._admit_one(sr)
                decisions.append(decision)
            # Если заявка в waiting_dependency — проверяем зависимости
            elif sr["status"] == "waiting_dependency":
                all_done, unfinished = self.db.check_dependencies(sr["stage_request_id"])
                if all_done:
                    self.db.update_stage_request_status(sr["stage_request_id"], "queued")
                    decisions.append({
                        "stage_request_id": sr["stage_request_id"],
                        "decision": "queued",
                        "reason": "Зависимости выполнены"
                    })
        
        return {
            "expired_locks": expired_count,
            "reconsidered_requests": len(decisions),
            "decisions": decisions
        }