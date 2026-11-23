PRAGMA foreign_keys = ON;

-- Шаблон бизнес-процесса
-- route: JSON для MVP (упорядоченный список stage_id с правилами перехода)
CREATE TABLE business_process (
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
CREATE TABLE stage (
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
CREATE TABLE bp_request (
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
CREATE TABLE stage_request (
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
CREATE TABLE run (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  stage_request_id INTEGER NOT NULL UNIQUE,
  status TEXT NOT NULL,
  external_event TEXT,  -- JSON {actor, expected_decision, received_at, decision, comment}; NULL пока ожидание не завершено
  created_at DATETIME NOT NULL,
  started_at DATETIME,
  finished_at DATETIME,
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
CREATE TABLE lock (
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
CREATE TABLE event (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  timestamp DATETIME NOT NULL,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id INTEGER NOT NULL,
  payload TEXT,
  actor TEXT
);

CREATE UNIQUE INDEX bp_request_index_0 ON bp_request (business_process_id, date, input_hash);
CREATE UNIQUE INDEX stage_request_index_1 ON stage_request (stage_id, date, input_hash);
CREATE INDEX lock_index_2 ON lock (resource_id, granularity, data_key, state);
CREATE UNIQUE INDEX lock_unique_idx ON lock (run_id, resource_id, granularity, data_key);
CREATE INDEX event_index_3 ON event (entity_type, entity_id, timestamp);
CREATE INDEX event_index_4 ON event (event_type, timestamp);

-- Комментарии к схеме:
-- business_process: шаблон бизнес-процесса
-- stage: этап бизнес-процесса
-- bp_request: заявка на бизнес-процесс
-- stage_request: заявка на этап; уникальный индекс схлопывает дубликаты на уровне этапа, в том числе между разными бизнес-процессами
-- run: запуск этапа; stage_request_id unique = один запуск на заявку
-- lock: учётная таблица оркестратора (реестр намерений и фактов захвата), не механизм блокировок СУБД
-- event: журнал событий
-- input_hash в bp_request: ключ дубликатов SHA256(business_process_id + date + norm(input_payload))
-- input_hash в stage_request: ключ дубликатов SHA256(stage_id + date + norm(input_payload))
-- route и required_resources в stage: JSON для MVP, отдельные таблицы — задел на будущее