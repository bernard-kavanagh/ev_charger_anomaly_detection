-- ============================================================================
-- EV CHARGER IoT PLATFORM — UNIFIED TiDB CLOUD SCHEMA (v2)
-- Single cluster: data plane + context plane
-- Includes: FULLTEXT indexes, anomaly_breakdown, supersession tracking
-- ============================================================================

-- ============================================================================
-- DATA PLANE: IoT telemetry, Flink windows, outage catalog
-- ============================================================================

CREATE TABLE IF NOT EXISTS charger_telemetry (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  charger_id      VARCHAR(32)   NOT NULL,
  connector_id    TINYINT       NOT NULL DEFAULT 1,
  ts              TIMESTAMP(3)  NOT NULL,
  status          ENUM('Available','Preparing','Charging','SuspendedEVSE',
                       'SuspendedEV','Finishing','Faulted','Unavailable')  NOT NULL,
  transaction_id  VARCHAR(64),
  energy_wh       BIGINT,
  power_w         INT,
  voltage_v       DECIMAL(6,2),
  current_a       DECIMAL(6,2),
  soc_percent     TINYINT UNSIGNED,
  temp_c          DECIMAL(5,2),
  error_code      VARCHAR(32)   DEFAULT 'NoError',
  vendor_error    VARCHAR(128),
  fan_rpm         INT,
  earth_leak_ma   DECIMAL(5,2),
  contactor_cycles INT,
  signal_dbm      SMALLINT,
  INDEX idx_charger_ts      (charger_id, ts),
  INDEX idx_status_ts       (status, ts),
  INDEX idx_error           (error_code, ts)
);

CREATE TABLE IF NOT EXISTS charger_windows (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  charger_id      VARCHAR(32)   NOT NULL,
  window_start    TIMESTAMP     NOT NULL,
  window_end      TIMESTAMP     NOT NULL,
  msg_count       INT,
  avg_power_w     DECIMAL(10,2),
  max_power_w     INT,
  min_voltage_v   DECIMAL(6,2),
  max_voltage_v   DECIMAL(6,2),
  voltage_stddev  DECIMAL(6,3),
  avg_current_a   DECIMAL(6,2),
  max_temp_c      DECIMAL(5,2),
  avg_temp_c      DECIMAL(5,2),
  error_count     INT           DEFAULT 0,
  status_changes  INT           DEFAULT 0,
  distinct_errors JSON,
  avg_fan_rpm     INT,
  max_earth_leak  DECIMAL(5,2),
  anomaly_flags   JSON,
  anomaly_score   DECIMAL(4,3)  DEFAULT 0.000,
  -- v2: per-feature anomaly score breakdown for explainability
  anomaly_breakdown JSON        COMMENT 'Per-feature scores, e.g. {"voltage_instability": 0.18}',
  signature_vec   VECTOR(384),
  INDEX idx_charger_window  (charger_id, window_start),
  INDEX idx_anomaly         (anomaly_score DESC, window_start),
  VECTOR INDEX idx_sig_vec  ((VEC_COSINE_DISTANCE(signature_vec)))
);

CREATE TABLE IF NOT EXISTS outage_catalog (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  pattern_id      VARCHAR(32)   NOT NULL UNIQUE,
  pattern_name    VARCHAR(128)  NOT NULL,
  category        ENUM('electrical','mechanical','firmware','network',
                       'thermal','environmental','unknown') NOT NULL,
  root_cause      TEXT          NOT NULL,
  symptoms        JSON          NOT NULL,
  resolution      TEXT          NOT NULL,
  severity        ENUM('degraded','offline','safety') NOT NULL,
  affected_models JSON,
  affected_firmware JSON,
  occurrence_count INT          DEFAULT 1,
  last_seen       TIMESTAMP,
  created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  signature_vec   VECTOR(384),
  INDEX idx_category        (category, severity),
  VECTOR INDEX idx_outage_vec ((VEC_COSINE_DISTANCE(signature_vec))),
  -- v2: FULLTEXT for hybrid search (one column per index — TiDB limitation)
  FULLTEXT INDEX ft_outage_root_cause (root_cause),
  FULLTEXT INDEX ft_outage_resolution (resolution)
);

CREATE TABLE IF NOT EXISTS charger_registry (
  charger_id      VARCHAR(32)   PRIMARY KEY,
  site_id         VARCHAR(64)   NOT NULL,
  model           VARCHAR(64),
  manufacturer    VARCHAR(64),
  firmware_version VARCHAR(16),
  install_date    DATE,
  lat             DECIMAL(9,6),
  lon             DECIMAL(9,6),
  connector_count TINYINT       DEFAULT 1,
  max_power_kw    DECIMAL(5,1),
  environment     ENUM('indoor','outdoor_sheltered','outdoor_exposed','coastal'),
  last_maintenance DATE,
  total_sessions  INT           DEFAULT 0,
  total_energy_kwh BIGINT       DEFAULT 0,
  INDEX idx_site            (site_id),
  INDEX idx_model           (manufacturer, model)
);


-- ============================================================================
-- CONTEXT PLANE: Agent reasoning, fleet memory, session state
-- ============================================================================

CREATE TABLE IF NOT EXISTS agent_reasoning (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  charger_id      VARCHAR(32),
  site_id         VARCHAR(64),
  session_id      VARCHAR(64)   NOT NULL,
  created_at      TIMESTAMP(3)  DEFAULT CURRENT_TIMESTAMP(3),
  observation     TEXT          NOT NULL,
  hypothesis      TEXT,
  evidence_refs   JSON,
  confidence      DECIMAL(3,2)  DEFAULT 0.50,
  resolution      ENUM('confirmed','dismissed','escalated','promoted')
                                NOT NULL,
  resolved_at     TIMESTAMP     NULL,
  tags            JSON,
  -- v2: contradiction tracking
  superseded_by   BIGINT        NULL COMMENT 'ID of newer reasoning that contradicts this one',
  superseded_at   TIMESTAMP     NULL COMMENT 'When this reasoning was superseded',
  reasoning_vec   VECTOR(384),
  INDEX idx_charger_reasoning (charger_id, created_at DESC),
  INDEX idx_session          (session_id, created_at),
  INDEX idx_resolution       (resolution, created_at DESC),
  VECTOR INDEX idx_reasoning_vec ((VEC_COSINE_DISTANCE(reasoning_vec))),
  -- v2: FULLTEXT for hybrid search
  FULLTEXT INDEX ft_reasoning_obs (observation)
);

CREATE TABLE IF NOT EXISTS fleet_memory (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  updated_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  category        ENUM('pattern','preference','hardware_note','site_context',
                       'firmware_bug','seasonal','operational_rule') NOT NULL,
  scope           VARCHAR(128)  NOT NULL DEFAULT 'global',
  content         TEXT          NOT NULL,
  source_refs     JSON,
  confidence      DECIMAL(3,2)  DEFAULT 0.70,
  supporting_evidence_count INT DEFAULT 1,
  access_count    INT           DEFAULT 0,
  last_accessed   TIMESTAMP     NULL,
  status          ENUM('active','deprecated','superseded') DEFAULT 'active',
  superseded_by   BIGINT        NULL,
  memory_vec      VECTOR(384),
  INDEX idx_scope_status    (scope, status),
  INDEX idx_category        (category, status),
  VECTOR INDEX idx_memory_vec ((VEC_COSINE_DISTANCE(memory_vec))),
  -- v2: FULLTEXT for hybrid search
  FULLTEXT INDEX ft_memory_content (content)
);

CREATE TABLE IF NOT EXISTS session_state (
  session_id      VARCHAR(64)   PRIMARY KEY,
  user_id         VARCHAR(64),
  started_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  last_active     TIMESTAMP     DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  focus_chargers  JSON,
  focus_site      VARCHAR(64),
  investigation_summary TEXT,
  token_budget    INT           DEFAULT 4000,
  tokens_used     INT           DEFAULT 0,
  last_context_hash VARCHAR(64),
  INDEX idx_user            (user_id, last_active DESC),
  INDEX idx_active          (last_active)
);

CREATE TABLE IF NOT EXISTS context_snapshots (
  id              BIGINT AUTO_RANDOM PRIMARY KEY,
  entity_type     ENUM('charger','site','fleet_summary') NOT NULL,
  entity_id       VARCHAR(64)   NOT NULL,
  snapshot_type   ENUM('profile','recent_anomalies','maintenance_history',
                       'performance_baseline','active_investigations') NOT NULL,
  content         TEXT          NOT NULL,
  token_count     INT           NOT NULL DEFAULT 0,
  created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
  expires_at      TIMESTAMP     NOT NULL,
  is_stale        BOOLEAN       DEFAULT FALSE,
  snapshot_vec    VECTOR(384),
  INDEX idx_entity          (entity_type, entity_id, snapshot_type),
  INDEX idx_expires         (expires_at),
  UNIQUE INDEX idx_unique_snap (entity_id, snapshot_type)
);


-- ============================================================================
-- TTL POLICIES (uncomment for production — do NOT enable during demos)
-- ============================================================================
-- ALTER TABLE charger_telemetry TTL = `ts` + INTERVAL 7 DAY TTL_JOB_INTERVAL = '1h';
-- ALTER TABLE charger_windows TTL = `window_start` + INTERVAL 30 DAY TTL_JOB_INTERVAL = '6h';
-- ALTER TABLE session_state TTL = `last_active` + INTERVAL 1 DAY TTL_JOB_INTERVAL = '1h';
-- ALTER TABLE context_snapshots TTL = `expires_at` + INTERVAL 0 DAY TTL_JOB_INTERVAL = '30m';
-- Steady-state with TTL: ~960M rows (vs unbounded 2B+/month without)
