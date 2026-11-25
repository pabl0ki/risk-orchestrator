"""Модуль допуска заявок на этапы."""
from backend.database.connection import OrchestratorDB


class Admit:
    """Принимает решение по заявке на этап: запустить / в очередь / отклонить."""

    def __init__(self, db: OrchestratorDB):
        self.db = db

    def admit_pending(self, limit: int = 100) -> list[dict]:
        """
        Обрабатывает заявки в статусах created/queued.
        
        Возвращает список принятых решений.
        """
        decisions = []
        pending = self.db.get_pending_stage_requests(limit)
        
        for sr in pending:
            decision = self._admit_one(sr)
            decisions.append(decision)
        
        return decisions

    def _admit_one(self, stage_request: dict) -> dict:
        """
        Проверяет одну заявку по четырём правилам.
        """
        sr_id = stage_request["stage_request_id"]
        stage_id = stage_request["stage_id"]
        date = stage_request["date"]
        
        # Правило 1: Дубликат (проверяется при создании, но на всякий случай)
        # Если stage_request уже существует, значит дубликата нет
        
        # Правило 2: Зависимости
        all_done, unfinished = self.db.check_dependencies(sr_id)
        if not all_done:
            self.db.update_stage_request_status(
                sr_id, "waiting_dependency", 
                {"type": "dependency", "ids": unfinished}
            )
            return {"stage_request_id": sr_id, "decision": "waiting_dependency", "reason": f"Ждёт родителей: {unfinished}"}
        
        # Правило 3: Ресурсы
        stage = self.db.get_stage(stage_id)
        if not stage:
            self.db.update_stage_request_status(sr_id, "rejected", {"type": "config", "reason": "Этап не найден"})
            return {"stage_request_id": sr_id, "decision": "rejected", "reason": "Этап не найден"}
        
        # Парсим required_resources
        import json
        required_resources = json.loads(stage.get("required_resources") or "[]")
        
        if not required_resources:
            # Нет ресурсов — сразу создаём run
            return self._approve_and_create_run(sr_id, stage_id, date, [])
        
        # Проверяем конфликты
        conflicts = []
        for resource in required_resources:
            resource_id = resource["resource_id"]
            data_key = resource.get("data_key") or date
            mode = resource["mode"]
            
            active_locks = self.db.get_active_locks(resource_id, data_key)
            for lock in active_locks:
                if not (lock["mode"] == "read" and mode == "read"):
                    conflicts.append({
                        "resource_id": resource_id,
                        "data_key": data_key,
                        "held_by": lock["run_id"],
                        "mode": lock["mode"]
                    })
        
        if conflicts:
            self.db.update_stage_request_status(
                sr_id, "waiting_resource",
                {"type": "resource", "conflicts": conflicts}
            )
            return {"stage_request_id": sr_id, "decision": "waiting_resource", "reason": f"Ресурсы заняты: {conflicts}"}
        
        # Правило 4: Атомарный захват
        locks = [
            {
                "resource_id": r["resource_id"],
                "granularity": r.get("granularity") or "partition",
                "data_key": r.get("data_key") or date,
                "mode": r["mode"]
            }
            for r in required_resources
        ]
        
        success = self.db.acquire_locks(sr_id, locks)  # Временно используем sr_id как run_id
        if not success:
            # Гонка — кто-то захватил ресурсы между проверкой и захватом
            self.db.update_stage_request_status(sr_id, "queued")
            return {"stage_request_id": sr_id, "decision": "queued", "reason": "Гонка при захвате"}
        
        return self._approve_and_create_run(sr_id, stage_id, date, locks)

    def _approve_and_create_run(self, sr_id: int, stage_id: int, date: str, locks: list[dict]) -> dict:
        """Одобряет заявку и создаёт запуск."""
        # Обновляем статус stage_request
        self.db.update_stage_request_status(sr_id, "approved")
        
        # Создаём run
        run_id = self.db.create_run(sr_id)
        
        # Логируем
        self.db.log_event(
            "run_created", "run", run_id,
            {"stage_request_id": sr_id, "stage_id": stage_id, "date": date, "locks": len(locks)},
            "system"
        )
        
        return {"stage_request_id": sr_id, "decision": "approved", "run_id": run_id}