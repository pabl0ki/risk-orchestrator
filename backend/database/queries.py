"""SQL-запросы к базе данных оркестратора.

Все запросы используют плейсхолдеры '?' для SQLite (защита от SQL-инъекций).
"""

# --- Справочники ---

INSERT_BUSINESS_PROCESS = """
    INSERT INTO business_process (name, risk_type, route, created_at) 
    VALUES (?, ?, ?, ?)
"""

GET_BUSINESS_PROCESS = """
    SELECT * FROM business_process WHERE business_process_id = ?
"""

GET_STAGES_BY_BP_ID = """
    SELECT * FROM stage WHERE business_process_id = ?
"""

# --- Заявки на бизнес-процесс ---

INSERT_BP_REQUEST = """
    INSERT INTO bp_request 
    (business_process_id, date, initiator_type, initiator_id, input_payload, input_hash, submitted_at, status) 
    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
"""

GET_BP_REQUEST_BY_HASH = """
    SELECT bp_request_id, status FROM bp_request 
    WHERE business_process_id=? AND date=? AND input_hash=?
"""

# --- Заявки на этапы ---

INSERT_STAGE_REQUEST = """
    INSERT INTO stage_request
    (bp_request_id, stage_id, date, priority, input_hash, submitted_at, status, parent_stage_request_ids)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""

GET_STAGE_REQUESTS_BY_BP_REQUEST = """
    SELECT * FROM stage_request WHERE bp_request_id = ?
"""

GET_STAGE_REQUEST_BY_ID = """
    SELECT * FROM stage_request WHERE stage_request_id = ?
"""

GET_PENDING_STAGE_REQUESTS = """
    SELECT * FROM stage_request
    WHERE status IN ('created', 'queued')
    ORDER BY priority DESC, submitted_at ASC
    LIMIT ?
"""

UPDATE_STAGE_REQUEST_STATUS = """
    UPDATE stage_request SET status = ?, waiting_for = ? WHERE stage_request_id = ?
"""

GET_STAGE_REQUESTS_BY_PARENT = """
    SELECT sr.* FROM stage_request sr, json_each(sr.parent_stage_request_ids) je
    WHERE je.value = ?
"""

# --- Запуски ---

INSERT_RUN = """
    INSERT INTO run (stage_request_id, status, created_at)
    VALUES (?, 'queued', ?)
"""

GET_RUN_BY_STAGE_REQUEST = """
    SELECT * FROM run WHERE stage_request_id = ?
"""

UPDATE_RUN_STATUS = """
    UPDATE run 
    SET status = ?, 
        started_at = CASE WHEN ? IS NOT NULL THEN ? ELSE started_at END,
        finished_at = CASE WHEN ? IS NOT NULL THEN ? ELSE finished_at END
    WHERE run_id = ?
"""

GET_RUNS_BY_STATUS = """
    SELECT * FROM run WHERE status = ?
"""

# --- Блокировки ---

CHECK_LOCK_CONFLICT = """
    SELECT 1 FROM lock 
    WHERE resource_id=? AND data_key=? AND state='held' 
    AND NOT (mode='read' AND ?='read')
"""

INSERT_LOCK = """
    INSERT INTO lock 
    (resource_id, granularity, data_key, run_id, mode, acquired_at, expires_at, state)
    VALUES (?, ?, ?, ?, ?, ?, datetime('now', '+30 minutes'), 'held')
"""

UPDATE_LOCK_STATE = """
    UPDATE lock
    SET state = ?, released_at = ?
    WHERE run_id = ? AND state = 'held'
"""

GET_LOCKS_BY_RUN = """
    SELECT * FROM lock WHERE run_id = ?
"""

GET_ACTIVE_LOCKS = """
    SELECT * FROM lock
    WHERE resource_id = ? AND data_key = ? AND state = 'held'
"""

GET_STAGES_BY_IDS = """
    SELECT * FROM stage WHERE stage_id IN ({})
"""

# --- Журнал событий ---

INSERT_EVENT = """
    INSERT INTO event (timestamp, event_type, entity_type, entity_id, payload, actor)
    VALUES (?, ?, ?, ?, ?, ?)
"""