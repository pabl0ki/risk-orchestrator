PRAGMA foreign_keys = ON;

-- Шаблон бизнес-процесса
-- route: JSON для MVP (упорядоченный список stage_id с правилами перехода)
CREATE TABLE IF NOT EXISTS business_process (
  business_process_id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  risk_type TEXT,
  route TEXT,
  default_priority INTEGER,
  created_at DATETIME
);

-- Этап бизнес-процесса
-- stage_type: calculation / approval / sign-off / publication
-- required_resources: JSON для MVP, список {resource_id, mode, granularity}
-- depends_on_stages: JSON, список stage_id предшественников
-- avg_duration: секунды
CREATE TABLE IF NOT EXISTS stage (
  stage_id INTEGER PRIMARY KEY AUTOINCREMENT,
  business_process_id INTEGER NOT NULL,
  name TEXT NOT NULL,
  stage_type TEXT,
  required_resources TEXT,
  depends_on_stages TEXT,
  avg_duration INTEGER,
  priority_default INTEGER,
  FOREIGN KEY (business_process_id) REFERENCES business_process (business_process_id)
);

-- Заявка на бизнес-процесс
-- initiator_type: manual / schedule; initiator_id: user_id или schedule_id
-- input_hash: ключ дубликатов SHA256(business_process_id + date + norm(input_payload))
-- status: pending / expanded / in_progress / done / failed / merged / rejected
CREATE TABLE IF NOT EXISTS bp_request (
  bp_request_id INTEGER PRIMARY KEY AUTOINCREMENT,
  business_process_id INTEGER NOT NULL,
  date DATE NOT NULL,
  initiator_type TEXT NOT NULL,
  initiator_id TEXT NOT NULL,
  input_payload TEXT,
  input_hash TEXT NOT NULL,
  priority INTEGER,
  submitted_at DATETIME NOT NULL,
  status TEXT NOT NULL,
  merged_into_bp_request_id INTEGER,
  FOREIGN KEY (business_process_id) REFERENCES business_process (business_process_id),
  FOREIGN KEY (merged_into_bp_request_id) REFERENCES bp_request (bp_request_id)
);

-- Заявка на этап
-- input_hash: ключ дубликатов SHA256(stage_id + date + norm(input_payload)); режим доступа и ресурсы НЕ входят в хэш
-- status: created / waiting_dependency / queued / approved / merged / rejected
-- waiting_for: JSON {type: resource|dependency, id: ...}
CREATE TABLE IF NOT EXISTS stage_request (
  stage_request_id INTEGER PRIMARY KEY AUTOINCREMENT,
  bp_request_id INTEGER NOT NULL,
  stage_id INTEGER NOT NULL,
  date DATE NOT NULL,
  priority INTEGER,
  input_hash TEXT NOT NULL,
  submitted_at DATETIME NOT NULL,
  status TEXT NOT NULL,
  waiting_for TEXT,
  parent_stage_request_ids TEXT,  -- JSON-массив ID родительских stage_request; NULL для корневых этапов
  rejection_reason TEXT,
  merged_into_stage_request_id INTEGER,
  FOREIGN KEY (bp_request_id) REFERENCES bp_request (bp_request_id),
  FOREIGN KEY (stage_id) REFERENCES stage (stage_id),
  FOREIGN KEY (merged_into_stage_request_id) REFERENCES stage_request (stage_request_id)
);

-- Запуск этапа
-- stage_request_id UNIQUE: один запуск на заявку
-- status: queued / waiting_dependency / waiting_resource / running / waiting_external / done / failed / cancelled
-- parent_run_id: если схлопнут с дубликатом
CREATE TABLE IF NOT EXISTS run (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stage_request_id INTEGER NOT NULL UNIQUE,
  status TEXT NOT NULL,
  external_event TEXT,  -- JSON {actor, expected_decision, received_at, decision, comment}; NULL пока ожидание не завершено
  created_at DATETIME NOT NULL,
  started_at DATETIME,
  finished_at DATETIME,
  heartbeat_at DATETIME,            -- время последнего пульса (heartbeat)
  lease_expires_at DATETIME,        -- до какого момента действует аренда запуска
  parent_run_id INTEGER,
  FOREIGN KEY (stage_request_id) REFERENCES stage_request (stage_request_id),
  FOREIGN KEY (parent_run_id) REFERENCES run (run_id)
);

-- Учётная таблица оркестратора (реестр намерений и фактов захвата),
-- не механизм блокировок СУБД
-- granularity: table / partition (row — задел, в MVP не используется)
-- data_key: идентификатор партиции; для блокировки всей таблицы всегда 'ALL' (NULL не использовать)
-- mode: read / write / exclusive; state: held / released / expired
-- expires_at: таймаут удержания, по умолчанию 30 минут
CREATE TABLE IF NOT EXISTS lock (
  lock_id INTEGER PRIMARY KEY AUTOINCREMENT,
  resource_id TEXT NOT NULL,
  granularity TEXT NOT NULL,
  data_key TEXT NOT NULL DEFAULT 'ALL',
  run_id INTEGER NOT NULL,
  mode TEXT NOT NULL,
  acquired_at DATETIME NOT NULL,
  released_at DATETIME,
  expires_at DATETIME NOT NULL,
  state TEXT NOT NULL,
  FOREIGN KEY (run_id) REFERENCES run (run_id)
);

-- Журнал событий
-- entity_id: полиморфная ссылка (entity_type + entity_id), FK не задаётся
CREATE TABLE IF NOT EXISTS event (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp DATETIME NOT NULL,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  payload TEXT,
  actor TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS pending_bp_hash ON bp_request (business_process_id, date, input_hash) WHERE status='pending';
CREATE INDEX IF NOT EXISTS stage_hash ON stage_request (stage_id, date, input_hash);
CREATE INDEX IF NOT EXISTS lock_index_2 ON lock (resource_id, granularity, data_key, state);
CREATE UNIQUE INDEX IF NOT EXISTS lock_unique_idx ON lock (run_id, resource_id, granularity, data_key);
CREATE INDEX IF NOT EXISTS event_index_3 ON event (entity_type, entity_id, timestamp);
CREATE INDEX IF NOT EXISTS event_index_4 ON event (event_type, timestamp);

-- Комментарии к схеме:
-- business_process: шаблон бизнес-процесса
-- stage: этап бизнес-процесса
-- bp_request: заявка на бизнес-процесс
-- Дубликат заявки процесса ссылается на оригинал и использует его этапы.
-- run: запуск этапа; stage_request_id unique = один запуск на заявку
-- lock: учётная таблица оркестратора (реестр намерений и фактов захвата), не механизм блокировок СУБД
-- event: журнал событий
-- input_hash в bp_request: ключ дубликатов SHA256(business_process_id + date + norm(input_payload))
-- input_hash в stage_request: ключ дубликатов SHA256(stage_id + date + norm(input_payload))
-- route и required_resources в stage: JSON для MVP, отдельные таблицы — задел на будущее
-- Public RUN numbers remain stable across all stages of a business request.
CREATE TABLE IF NOT EXISTS process_route (
  code TEXT PRIMARY KEY, business_process_id INTEGER NOT NULL UNIQUE REFERENCES business_process,
  revision INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS workflow (
  public_id INTEGER PRIMARY KEY, bp_request_id INTEGER NOT NULL UNIQUE REFERENCES bp_request,
  attempt INTEGER NOT NULL, token TEXT, lease REAL NOT NULL, inputs TEXT NOT NULL,
  result_version TEXT REFERENCES result_version(version) DEFERRABLE INITIALLY DEFERRED,
  reason TEXT NOT NULL, start_at REAL, end_at REAL, calculated_at REAL,
  wait_for INTEGER REFERENCES workflow, has_wait_for INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS request_receipt (
  request_key TEXT PRIMARY KEY, bp_request_id INTEGER NOT NULL UNIQUE REFERENCES bp_request,
  public_id INTEGER NOT NULL REFERENCES workflow
);
CREATE TABLE IF NOT EXISTS result_version (
  version TEXT PRIMARY KEY, public_id INTEGER NOT NULL REFERENCES workflow,
  resource TEXT NOT NULL, scope TEXT, previous TEXT REFERENCES result_version,
  rows_json TEXT NOT NULL, inputs TEXT NOT NULL, algorithm TEXT NOT NULL, factor INTEGER NOT NULL,
  token TEXT, approval TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS published_result (
  resource_key TEXT PRIMARY KEY, version TEXT NOT NULL REFERENCES result_version
);
CREATE TABLE IF NOT EXISTS decision_receipt (
  command_key TEXT PRIMARY KEY, public_id INTEGER NOT NULL REFERENCES workflow,
  decision TEXT NOT NULL, actor TEXT NOT NULL, successor INTEGER REFERENCES workflow,
  comment TEXT, version TEXT, signature_length INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS schedule (
  id INTEGER PRIMARY KEY CHECK(id=1), process_code TEXT NOT NULL REFERENCES process_route,
  business_date TEXT NOT NULL, period INTEGER NOT NULL, due REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS demo_batch (
  batch_id INTEGER PRIMARY KEY, scenario TEXT NOT NULL, business_date TEXT NOT NULL, requests INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS demo_batch_job (
  batch_id INTEGER NOT NULL REFERENCES demo_batch, public_id INTEGER NOT NULL REFERENCES workflow,
  position INTEGER NOT NULL, PRIMARY KEY(batch_id, public_id)
);
CREATE TABLE IF NOT EXISTS workflow_event (
  event_id INTEGER PRIMARY KEY REFERENCES event, seq INTEGER NOT NULL UNIQUE
);

CREATE TRIGGER IF NOT EXISTS log_lock_acquired AFTER INSERT ON lock WHEN NEW.state='held'
BEGIN
  INSERT INTO event(timestamp,event_type,entity_type,entity_id,payload,actor)
  VALUES(NEW.acquired_at,'lock_acquired','lock',NEW.lock_id,
         json_object('run_id',NEW.run_id,'resource',NEW.resource_id,'scope',NEW.data_key,'mode',NEW.mode),'system');
END;
CREATE TRIGGER IF NOT EXISTS log_lock_change AFTER UPDATE OF state ON lock WHEN OLD.state<>NEW.state
BEGIN
  INSERT INTO event(timestamp,event_type,entity_type,entity_id,payload,actor)
  VALUES(CURRENT_TIMESTAMP,CASE NEW.state WHEN 'held' THEN 'lock_acquired'
         WHEN 'expired' THEN 'lock_expired' ELSE 'lock_released' END,'lock',NEW.lock_id,
         json_object('run_id',NEW.run_id,'resource',NEW.resource_id,'scope',NEW.data_key),'system');
END;
