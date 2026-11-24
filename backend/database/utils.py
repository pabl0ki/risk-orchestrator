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