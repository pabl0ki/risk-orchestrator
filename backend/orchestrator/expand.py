"""Модуль разворачивания bp_request в stage_request."""
from backend.database.connection import OrchestratorDB
from backend.database.utils import validate_dag, calculate_hash


class Expand:
    """Разворачивает заявку на бизнес-процесс в заявки на этапы."""

    def __init__(self, db: OrchestratorDB):
        self.db = db

    def expand_bp_request(self, bp_request_id: int) -> dict:
        """
        Разворачивает bp_request в stage_request для всех этапов маршрута.
        
        Возвращает:
        - {"status": "expanded", "stage_requests": [...]} — успех
        - {"status": "rejected", "reason": "..."} — ошибка валидации
        """
        # 1. Читаем bp_request
        bp_request = self.db.conn.execute(
            "SELECT * FROM bp_request WHERE bp_request_id = ?", (bp_request_id,)
        ).fetchone()
        
        if not bp_request:
            return {"status": "rejected", "reason": "bp_request не найден"}
        
        if bp_request["status"] != "pending":
            return {"status": "rejected", "reason": f"Неверный статус: {bp_request['status']}"}
        
        # 2. Читаем бизнес-процесс и его этапы
        bp = self.db.get_business_process(bp_request["business_process_id"])
        if not bp:
            return {"status": "rejected", "reason": "Бизнес-процесс не найден"}
        
        stages = self.db.get_stages(bp["business_process_id"])
        if not stages:
            return {"status": "rejected", "reason": "Нет этапов в маршруте"}
        
        # 3. Валидируем DAG
        is_valid, error = validate_dag(stages)
        if not is_valid:
            # Отклоняем bp_request
            self.db.conn.execute(
                "UPDATE bp_request SET status = 'rejected', merged_into_bp_request_id = NULL WHERE bp_request_id = ?",
                (bp_request_id,)
            )
            self.db.log_event(
                "bp_request_rejected", "bp_request", bp_request_id,
                {"reason": error}, "system"
            )
            return {"status": "rejected", "reason": error}
        
        # 4. Создаём stage_request для каждого этапа (жадно)
        stage_requests = []
        for stage in stages:
            # Определяем родителей
            depends_on = stage.get("depends_on_stages") or []
            parent_stage_request_ids = []
            
            if depends_on:
                # Находим stage_request для каждого родителя
                for parent_stage_id in depends_on:
                    parent_sr = self.db.conn.execute(
                        """SELECT stage_request_id FROM stage_request 
                           WHERE bp_request_id = ? AND stage_id = ?""",
                        (bp_request_id, parent_stage_id)
                    ).fetchone()
                    if parent_sr:
                        parent_stage_request_ids.append(parent_sr["stage_request_id"])
            
            # Считаем хэш для этапа (payload наследуется от bp_request)
            input_hash = calculate_hash(
                stage["stage_id"], 
                bp_request["date"], 
                bp_request["input_payload"]
            )
            
            # Создаём stage_request
            sr_id = self.db.create_stage_request(
                bp_request_id=bp_request_id,
                stage_id=stage["stage_id"],
                date=bp_request["date"],
                priority=stage.get("priority_default") or bp_request.get("priority") or 10,
                input_hash=input_hash,
                parent_stage_request_ids=parent_stage_request_ids if parent_stage_request_ids else None
            )
            
            stage_requests.append({
                "stage_request_id": sr_id,
                "stage_id": stage["stage_id"],
                "status": "created" if not parent_stage_request_ids else "waiting_dependency"
            })
        
        # 5. Обновляем статус bp_request
        self.db.conn.execute(
            "UPDATE bp_request SET status = 'expanded' WHERE bp_request_id = ?",
            (bp_request_id,)
        )
        
        # 6. Логируем событие
        self.db.log_event(
            "bp_request_expanded", "bp_request", bp_request_id,
            {"stage_requests": len(stage_requests)}, "system"
        )
        
        return {"status": "expanded", "stage_requests": stage_requests}