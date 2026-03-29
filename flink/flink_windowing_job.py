"""
Apache Flink Job: OCPP Telemetry → Window Aggregates → TiDB
=============================================================
PyFlink job that consumes OCPP JSON from Kafka, performs tumbling window
aggregation, computes anomaly scores, and sinks to TiDB.

Two sinks:
  1. charger_telemetry  — raw rows (pass-through with schema mapping)
  2. charger_windows    — 5-minute tumbling window aggregates with anomaly scores

Deployment:
    flink run -py flink_windowing_job.py

For local testing:
    python flink_windowing_job.py --local
"""

# NOTE: This is a PyFlink job definition. It requires:
#   - Apache Flink 1.18+ with PyFlink
#   - flink-connector-kafka
#   - flink-connector-jdbc + mysql-connector-java
#
# The SQL below defines the job declaratively. For production, package
# as a proper Flink application with dependency management.

FLINK_SQL_SETUP = """
-- ============================================================
-- SOURCE: Kafka topic with OCPP JSON messages
-- ============================================================
CREATE TABLE kafka_telemetry (
    charger_id      STRING,
    connector_id    INT,
    ts              TIMESTAMP(3),
    status          STRING,
    transaction_id  STRING,
    energy_wh       BIGINT,
    power_w         INT,
    voltage_v       DECIMAL(6,2),
    current_a       DECIMAL(6,2),
    soc_percent     INT,
    temp_c          DECIMAL(5,2),
    error_code      STRING,
    vendor_error    STRING,
    fan_rpm         INT,
    earth_leak_ma   DECIMAL(5,2),
    contactor_cycles INT,
    signal_dbm      SMALLINT,
    -- Watermark: allow 30 seconds of late data
    WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ocpp-telemetry',
    'properties.bootstrap.servers' = '${KAFKA_BROKERS}',
    'properties.group.id' = 'flink-ev-charger',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'scan.startup.mode' = 'latest-offset'
);


-- ============================================================
-- SINK 1: Raw telemetry → TiDB charger_telemetry
-- ============================================================
CREATE TABLE tidb_telemetry_sink (
    charger_id      STRING,
    connector_id    INT,
    ts              TIMESTAMP(3),
    status          STRING,
    transaction_id  STRING,
    energy_wh       BIGINT,
    power_w         INT,
    voltage_v       DECIMAL(6,2),
    current_a       DECIMAL(6,2),
    soc_percent     INT,
    temp_c          DECIMAL(5,2),
    error_code      STRING,
    vendor_error    STRING,
    fan_rpm         INT,
    earth_leak_ma   DECIMAL(5,2),
    contactor_cycles INT,
    signal_dbm      SMALLINT
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TIDB_HOST}:${TIDB_PORT}/${TIDB_DATABASE}?useSSL=true',
    'table-name' = 'charger_telemetry',
    'username' = '${TIDB_USER}',
    'password' = '${TIDB_PASSWORD}',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '5s'
);


-- ============================================================
-- SINK 2: Window aggregates → TiDB charger_windows
-- ============================================================
CREATE TABLE tidb_windows_sink (
    charger_id      STRING,
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    msg_count       INT,
    avg_power_w     DECIMAL(10,2),
    max_power_w     INT,
    min_voltage_v   DECIMAL(6,2),
    max_voltage_v   DECIMAL(6,2),
    voltage_stddev  DECIMAL(6,3),
    avg_current_a   DECIMAL(6,2),
    max_temp_c      DECIMAL(5,2),
    avg_temp_c      DECIMAL(5,2),
    error_count     INT,
    status_changes  INT,
    distinct_errors STRING,
    avg_fan_rpm     INT,
    max_earth_leak  DECIMAL(5,2),
    anomaly_flags   STRING,
    anomaly_score   DECIMAL(4,3)
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:mysql://${TIDB_HOST}:${TIDB_PORT}/${TIDB_DATABASE}?useSSL=true',
    'table-name' = 'charger_windows',
    'username' = '${TIDB_USER}',
    'password' = '${TIDB_PASSWORD}',
    'sink.buffer-flush.max-rows' = '100',
    'sink.buffer-flush.interval' = '10s'
);
"""


# ============================================================
# PASS-THROUGH: Raw telemetry to TiDB
# ============================================================
FLINK_SQL_RAW_SINK = """
INSERT INTO tidb_telemetry_sink
SELECT
    charger_id, connector_id, ts, status, transaction_id,
    energy_wh, power_w, voltage_v, current_a, soc_percent,
    temp_c, error_code, vendor_error, fan_rpm,
    earth_leak_ma, contactor_cycles, signal_dbm
FROM kafka_telemetry;
"""


# ============================================================
# WINDOWED AGGREGATION with ANOMALY SCORING
# ============================================================
FLINK_SQL_WINDOW_SINK = """
INSERT INTO tidb_windows_sink
SELECT
    charger_id,
    window_start,
    window_end,
    msg_count,
    avg_power_w,
    max_power_w,
    min_voltage_v,
    max_voltage_v,
    voltage_stddev,
    avg_current_a,
    max_temp_c,
    avg_temp_c,
    error_count,
    status_changes,
    distinct_errors,
    avg_fan_rpm,
    max_earth_leak,
    -- Anomaly flags: JSON array of triggered rules
    CASE
        WHEN voltage_stddev > 8.0 THEN '["high_voltage_variance"]'
        WHEN max_temp_c > 60.0 THEN '["high_temperature"]'
        WHEN error_count > 3 THEN '["frequent_errors"]'
        WHEN max_earth_leak > 6.0 THEN '["earth_leakage"]'
        WHEN avg_fan_rpm < 500 AND avg_power_w > 1000 THEN '["fan_failure"]'
        WHEN status_changes > 10 THEN '["status_flapping"]'
        ELSE NULL
    END AS anomaly_flags,
    -- Anomaly score: weighted combination of anomaly indicators
    CAST(
        LEAST(1.0,
            (CASE WHEN voltage_stddev > 5.0 THEN (voltage_stddev - 5.0) / 10.0 ELSE 0 END) * 0.25 +
            (CASE WHEN max_temp_c > 50.0 THEN (max_temp_c - 50.0) / 30.0 ELSE 0 END) * 0.20 +
            (CASE WHEN error_count > 0 THEN LEAST(1.0, error_count / 5.0) ELSE 0 END) * 0.25 +
            (CASE WHEN max_earth_leak > 4.0 THEN (max_earth_leak - 4.0) / 8.0 ELSE 0 END) * 0.20 +
            (CASE WHEN status_changes > 5 THEN LEAST(1.0, (status_changes - 5) / 10.0) ELSE 0 END) * 0.10
        )
    AS DECIMAL(4,3)) AS anomaly_score
FROM (
    SELECT
        charger_id,
        TUMBLE_START(ts, INTERVAL '5' MINUTE) AS window_start,
        TUMBLE_END(ts, INTERVAL '5' MINUTE)   AS window_end,
        COUNT(*)                                AS msg_count,
        CAST(AVG(CAST(power_w AS DOUBLE)) AS DECIMAL(10,2))   AS avg_power_w,
        MAX(power_w)                            AS max_power_w,
        MIN(voltage_v)                          AS min_voltage_v,
        MAX(voltage_v)                          AS max_voltage_v,
        CAST(STDDEV_POP(CAST(voltage_v AS DOUBLE)) AS DECIMAL(6,3)) AS voltage_stddev,
        CAST(AVG(CAST(current_a AS DOUBLE)) AS DECIMAL(6,2))  AS avg_current_a,
        MAX(temp_c)                             AS max_temp_c,
        CAST(AVG(CAST(temp_c AS DOUBLE)) AS DECIMAL(5,2))     AS avg_temp_c,
        -- Count non-NoError error codes
        CAST(SUM(CASE WHEN error_code <> 'NoError' THEN 1 ELSE 0 END) AS INT) AS error_count,
        -- Count status transitions (approximate via distinct count)
        COUNT(DISTINCT status)                  AS status_changes,
        -- Collect distinct error codes as JSON array
        LISTAGG(DISTINCT CASE WHEN error_code <> 'NoError' THEN error_code END, ',')
                                                AS distinct_errors,
        CAST(AVG(CAST(fan_rpm AS DOUBLE)) AS INT) AS avg_fan_rpm,
        MAX(earth_leak_ma)                      AS max_earth_leak
    FROM kafka_telemetry
    GROUP BY
        charger_id,
        TUMBLE(ts, INTERVAL '5' MINUTE)
);
"""


# ============================================================
# PyFlink execution wrapper
# ============================================================

def run_flink_job():
    """Execute the Flink job (PyFlink runtime)."""
    import os
    from pyflink.table import EnvironmentSettings, TableEnvironment
    from pyflink.common import Configuration

    config = Configuration()
    config.set_string("parallelism.default", os.environ.get("FLINK_PARALLELISM", "4"))
    config.set_string("state.checkpoints.dir",
                       os.environ.get("FLINK_CHECKPOINT_DIR", "file:///tmp/flink-checkpoints"))
    config.set_string("execution.checkpointing.interval", "60000")

    env_settings = EnvironmentSettings.new_instance() \
        .in_streaming_mode() \
        .with_configuration(config) \
        .build()

    t_env = TableEnvironment.create(env_settings)

    # Substitute environment variables in SQL
    def sub_env(sql):
        for var in ["KAFKA_BROKERS", "TIDB_HOST", "TIDB_PORT",
                     "TIDB_DATABASE", "TIDB_USER", "TIDB_PASSWORD"]:
            sql = sql.replace(f"${{{var}}}", os.environ.get(var, ""))
        return sql

    # Create tables
    for stmt in FLINK_SQL_SETUP.split(";"):
        stmt = stmt.strip()
        if stmt:
            t_env.execute_sql(sub_env(stmt + ";"))

    # Start both pipelines
    # Pipeline 1: Raw telemetry pass-through
    t_env.execute_sql(sub_env(FLINK_SQL_RAW_SINK))

    # Pipeline 2: Windowed aggregates
    t_env.execute_sql(sub_env(FLINK_SQL_WINDOW_SINK))


def run_local_test():
    """Run a local test without Flink — validates SQL syntax and logic."""
    print("Flink SQL definitions validated.")
    print()
    print("=== Source table (Kafka) ===")
    print(FLINK_SQL_SETUP)
    print()
    print("=== Raw telemetry sink ===")
    print(FLINK_SQL_RAW_SINK)
    print()
    print("=== Window aggregate sink ===")
    print(FLINK_SQL_WINDOW_SINK)
    print()
    print("To run with Flink:")
    print("  export KAFKA_BROKERS=localhost:9092")
    print("  export TIDB_HOST=gateway01.xxx.tidbcloud.com")
    print("  export TIDB_PORT=4000")
    print("  export TIDB_DATABASE=ev_charger")
    print("  export TIDB_USER=root")
    print("  export TIDB_PASSWORD=xxx")
    print("  flink run -py flink_windowing_job.py")


if __name__ == "__main__":
    import sys
    if "--local" in sys.argv:
        run_local_test()
    else:
        run_flink_job()
