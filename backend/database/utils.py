"""Вспомогательные утилиты"""

import hashlib
import json


def normalize_payload(payload: dict) -> str:
    """Приводит JSON к единому виду: ключи по алфавиту, без лишних пробелов.
    
    Гарантирует, что {"a":1,"b":2} и {"b":2,"a":1} дают одинаковую строку.
    """
    if payload is None:
        return ""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def calculate_hash(entity_id: int, date: str, payload: dict) -> str:
    """SHA256(entity_id + date + norm(payload)).
    
    Универсальная: используется и для bp_request (entity_id = business_process_id),
    и для stage_request (entity_id = stage_id).
    Режим доступа и список ресурсов в хэш НЕ входят.
    """
    normalized = normalize_payload(payload)
    raw = f"{entity_id}{date}{normalized}"
    return hashlib.sha256(raw.encode()).hexdigest()

def validate_dag(stages: list[dict]) -> tuple[bool, str | None]:
    """
    Проверяет граф зависимостей этапов на наличие циклов.
    stages: список словарей вида {'stage_id': int, 'depends_on_stages': list[int] | None}
    Возвращает (is_valid: bool, error_message: str | None)
    """
    # Строим граф: stage_id -> список stage_id предшественников
    graph = {s["stage_id"]: (s.get("depends_on_stages") or []) for s in stages}
    
    # Состояния узлов: 0 = не посещён, 1 = в процессе обхода, 2 = завершён
    state = {stage_id: 0 for stage_id in graph}
    
    def dfs(node: int, path: list[int]) -> str | None:
        if state[node] == 2:
            return None  # Уже обработан, цикла нет
        if state[node] == 1:
            # Нашли цикл: возвращаем цепочку
            cycle_start = path.index(node)
            return " -> ".join(map(str, path[cycle_start:] + [node]))
        
        state[node] = 1
        path.append(node)
        
        for predecessor in graph[node]:
            if predecessor not in graph:
                return f"Зависимость от несуществующего этапа {predecessor}"
            cycle = dfs(predecessor, path)
            if cycle:
                return cycle
        
        path.pop()
        state[node] = 2
        return None

    # Проверяем все узлы
    for stage_id in graph:
        if state[stage_id] == 0:
            cycle = dfs(stage_id, [])
            if cycle:
                return False, cycle if cycle.startswith("Зависимость") else f"Обнаружена циклическая зависимость: {cycle}"
    
    return True, None