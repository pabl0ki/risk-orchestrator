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

GET_STAGES_BY_IDS = """
    SELECT * FROM stage WHERE stage_id IN ({})
"""

GET_ALL_BUSINESS_PROCESSES = """
    SELECT * FROM business_process 
    ORDER BY created_at DESC
"""

GET_STAGE_BY_ID = """
    SELECT * FROM stage WHERE stage_id = ?
"""

UPDATE_BUSINESS_PROCESS = """
    UPDATE business_process 
    SET name = ?, risk_type = ?, route = ? 
    WHERE business_process_id = ?
"""

# --- Заявки на бизнес-процесс ---

INSERT_BP_REQUEST = """
    INSERT INTO bp_request 
    (business_process_id, date, initiator_type, initiator_id, input_payload, input_hash, submitted_at, status) 
    VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
"""

GET_BP_REQUEST_BY_HASH = """
    SELECT bp_request_id, status FROM bp_request 
    WHERE business_process_id=? AND date=? AND input_hash=? AND status IN ('pending','expanded','running')
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

GET_STAGE_REQUESTS_BY_IDS = """
    SELECT * FROM stage_request WHERE stage_request_id IN ({})
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

GET_ALL_QUEUED_REQUESTS = """
    SELECT * FROM stage_request 
    WHERE status IN ('created', 'queued', 'waiting_dependency', 'waiting_resource')
    ORDER BY priority DESC, submitted_at ASC
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

UPDATE_RUN_FINISHED = """
    UPDATE run 
    SET status = ?, finished_at = ? 
    WHERE run_id = ?
"""

GET_RUNS_BY_STATUS = """
    SELECT * FROM run WHERE status = ?
"""

UPDATE_RUN_HEARTBEAT = """
    UPDATE run 
    SET heartbeat_at = datetime('now'), 
        lease_expires_at = datetime('now', '+30 seconds')
    WHERE run_id = ? AND status='running'
"""

GET_STALE_RUNS = """
    SELECT * FROM run 
    WHERE status = 'running' 
      AND (lease_expires_at <= datetime('now') OR heartbeat_at <= datetime('now', ?))
"""

# --- Внешние события ---

UPDATE_RUN_EXTERNAL_EVENT = """
    UPDATE run 
    SET external_event = ? 
    WHERE run_id = ?
"""

GET_RUNS_WAITING_EXTERNAL = """
    SELECT * FROM run 
    WHERE status = 'waiting_external' 
    ORDER BY created_at ASC
"""

# --- Блокировки ---

CHECK_LOCK_CONFLICT = """
    SELECT 1 FROM lock 
    WHERE resource_id=?1 AND (data_key=?2 OR data_key='ALL' OR ?2='ALL') AND state='held'
    AND NOT (mode='read' AND ?3='read')
"""

INSERT_LOCK = """
    INSERT INTO lock 
    (resource_id, granularity, data_key, run_id, mode, acquired_at, expires_at, state)
    VALUES (?, ?, ?, ?, ?, ?, datetime('now', '+30 seconds'), 'held')
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
    WHERE resource_id=?1 AND (data_key=?2 OR data_key='ALL' OR ?2='ALL') AND state='held'
"""

GET_EXPIRED_LOCKS = """
    SELECT * FROM lock 
    WHERE state = 'held' AND expires_at < datetime('now')
"""

EXPIRE_LOCKS = """
    UPDATE lock 
    SET state = 'expired' 
    WHERE state = 'held' AND expires_at < datetime('now')
"""

# --- Журнал событий ---

INSERT_EVENT = """
    INSERT INTO event (timestamp, event_type, entity_type, entity_id, payload, actor)
    VALUES (?, ?, ?, ?, ?, ?)
"""

GET_EVENTS_BY_ENTITY = """
    SELECT * FROM event 
    WHERE entity_type = ? AND entity_id = ? 
    ORDER BY timestamp DESC
"""

GET_EVENTS_BY_TYPE = """
    SELECT * FROM event 
    WHERE event_type = ? 
    ORDER BY timestamp DESC
"""

GET_RECENT_EVENTS = """
    SELECT * FROM event 
    ORDER BY timestamp DESC 
    LIMIT ?
"""

# --- UI / наблюдаемость ---

GET_BP_REQUEST_FULL = """
    SELECT 
        bp.business_process_id, bp.name, bp.risk_type, bp.route, bp.created_at,
        br.bp_request_id, br.date, br.initiator_type, br.initiator_id, 
        br.input_payload, br.input_hash, br.priority, br.submitted_at, br.status, 
        br.merged_into_bp_request_id,
        sr.stage_request_id, sr.stage_id, sr.priority AS sr_priority, 
        sr.input_hash AS sr_input_hash, sr.submitted_at AS sr_submitted_at, 
        sr.status AS sr_status, sr.waiting_for, sr.parent_stage_request_ids, 
        sr.merged_into_stage_request_id,
        r.run_id, r.status AS run_status, r.external_event, r.created_at AS run_created_at, 
        r.started_at, r.finished_at, r.heartbeat_at, r.lease_expires_at, r.parent_run_id,
        l.lock_id, l.resource_id, l.granularity, l.data_key, l.mode, 
        l.acquired_at, l.expires_at AS lock_expires_at, l.state AS lock_state
    FROM bp_request br
    JOIN business_process bp ON br.business_process_id = bp.business_process_id
    LEFT JOIN stage_request sr ON sr.bp_request_id = br.bp_request_id
    LEFT JOIN run r ON r.stage_request_id = sr.stage_request_id
    LEFT JOIN lock l ON l.run_id = r.run_id
    WHERE br.bp_request_id = ?
    ORDER BY sr.stage_request_id, r.run_id, l.lock_id
"""

GET_QUEUE_SNAPSHOT = """
    SELECT 
        sr.stage_request_id, sr.stage_id, sr.date, sr.priority, sr.input_hash, 
        sr.submitted_at, sr.status, sr.waiting_for, sr.parent_stage_request_ids,
        br.bp_request_id, br.initiator_id, br.initiator_type,
        bp.name AS process_name
    FROM stage_request sr
    JOIN bp_request br ON sr.bp_request_id = br.bp_request_id
    JOIN business_process bp ON br.business_process_id = bp.business_process_id
    WHERE sr.status IN ('created', 'queued', 'waiting_dependency', 'waiting_resource')
    ORDER BY sr.priority DESC, sr.submitted_at ASC
"""

GET_LOCKS_SNAPSHOT = """
    SELECT 
        l.lock_id, l.resource_id, l.granularity, l.data_key, l.mode, 
        l.acquired_at, l.expires_at, l.state, l.run_id,
        r.status AS run_status, r.stage_request_id,
        sr.stage_id, sr.date AS sr_date,
        bp.name AS process_name
    FROM lock l
    JOIN run r ON l.run_id = r.run_id
    JOIN stage_request sr ON r.stage_request_id = sr.stage_request_id
    JOIN bp_request br ON sr.bp_request_id = br.bp_request_id
    JOIN business_process bp ON br.business_process_id = bp.business_process_id
    WHERE l.state = 'held'
    ORDER BY l.resource_id, l.data_key, l.acquired_at
"""