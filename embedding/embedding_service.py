"""
Embedding Service (TiCDC → Kafka → Embed → TiDB writeback)
============================================================
Consumes TiCDC change events from Kafka, generates vector embeddings
for new rows in charger_windows and outage_catalog, and writes the
embeddings back to TiDB.

Architecture:
    TiDB (new row in charger_windows)
      → TiCDC captures INSERT
      → Kafka topic: ticdc-charger-windows
      → This service consumes the event
      → Calls embedding API (Voyage / OpenAI)
      → UPDATEs the signature_vec column in TiDB

Why TiCDC instead of doing this in Flink?
    Embedding API calls are external, high-latency (50-200ms), and
    rate-limited. Putting them in the Flink pipeline would create
    backpressure that slows the entire stream. TiCDC decouples the
    embedding step: Flink writes to TiDB at full speed, TiCDC picks
    up the changes asynchronously, and this service handles the API
    calls with its own concurrency and retry logic.

Usage:
    python embedding_service.py

    # With environment variables:
    export KAFKA_BROKERS=localhost:9092
    export TIDB_HOST=gateway01.xxx.tidbcloud.com
    export TIDB_PORT=4000
    export TIDB_USER=root
    export TIDB_PASSWORD=xxx
    export EMBEDDING_PROVIDER=voyage  # or 'openai'
    export EMBEDDING_BATCH_SIZE=32
    python embedding_service.py

TiCDC configuration:
    cdc cli changefeed create \\
        --sink-uri="kafka://kafka:9092/ticdc-charger-windows?protocol=canal-json" \\
        --config ticdc_config.toml

    # ticdc_config.toml:
    [filter]
    rules = ['ev_charger.charger_windows', 'ev_charger.outage_catalog',
             'ev_charger.agent_reasoning', 'ev_charger.fleet_memory']
"""

import argparse
import json
import os
import sys
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pymysql
from dotenv import load_dotenv
from langchain_community.embeddings import HuggingFaceEmbeddings

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("embedding-service")


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:9092")
TIDB_CONFIG = {
    "host": os.environ.get("TIDB_HOST", "localhost"),
    "port": int(os.environ.get("TIDB_PORT", "4000")),
    "user": os.environ.get("TIDB_USER", "root"),
    "password": os.environ.get("TIDB_PASSWORD", ""),
    "database": os.environ.get("TIDB_DATABASE", "ev_charger"),
}
EMBEDDING_PROVIDER = os.environ.get("EMBEDDING_PROVIDER", "huggingface")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
EMBEDDING_BATCH_SIZE = int(os.environ.get("EMBEDDING_BATCH_SIZE", "32"))
EMBEDDING_WORKERS = int(os.environ.get("EMBEDDING_WORKERS", "4"))

# Lazy-loaded HuggingFace model (same pattern as tidb-self-healing-db-agent)
_hf_embeddings = None

def _get_hf_embeddings() -> HuggingFaceEmbeddings:
    global _hf_embeddings
    if _hf_embeddings is None:
        log.info(f"Loading embedding model ({EMBEDDING_MODEL})...")
        _hf_embeddings = HuggingFaceEmbeddings(model_name=EMBEDDING_MODEL)
    return _hf_embeddings

# Topic → (table_name, text_builder_function, vector_column, id_column)
TOPIC_CONFIG = {
    "ticdc-charger-windows": {
        "table": "charger_windows",
        "vec_column": "signature_vec",
        "id_column": "id",
    },
    "ticdc-outage-catalog": {
        "table": "outage_catalog",
        "vec_column": "signature_vec",
        "id_column": "id",
    },
    "ticdc-agent-reasoning": {
        "table": "agent_reasoning",
        "vec_column": "reasoning_vec",
        "id_column": "id",
    },
    "ticdc-fleet-memory": {
        "table": "fleet_memory",
        "vec_column": "memory_vec",
        "id_column": "id",
    },
}


# ---------------------------------------------------------------------------
# Embedding providers
# ---------------------------------------------------------------------------

def embed_huggingface(texts: list[str]) -> list[list[float]]:
    """Generate embeddings using local HuggingFace sentence-transformers model."""
    return _get_hf_embeddings().embed_documents(texts)


EMBED_FN = embed_huggingface


# ---------------------------------------------------------------------------
# Text builders: convert row data into embeddable text
# ---------------------------------------------------------------------------

def _band_power(power_w, max_power_w, status) -> str:
    power_w = float(power_w or 0)
    max_power_w = float(max_power_w or 0)
    status = str(status or "")
    if power_w == 0:
        if status in ("Faulted", "Unavailable"):
            return "zero power delivery, charger protection-locked or offline"
        return "idle, no active session"
    if max_power_w > 0:
        if power_w < max_power_w * 0.5:
            return "reduced power output, possible thermal de-rating or cable degradation"
        return "normal power delivery"
    # Fallback absolute thresholds (DC charger)
    if power_w < 5000:
        return "reduced power output, possible thermal de-rating or cable degradation"
    if power_w <= 20000:
        return "normal power delivery"
    return "normal power delivery"


def _band_voltage(voltage_stddev) -> str:
    v = float(voltage_stddev or 0)
    if v <= 2.0:
        return "stable voltage"
    if v <= 5.0:
        return "minor voltage fluctuation, possible switching transients"
    if v <= 8.0:
        return "moderate voltage instability"
    return "high voltage variance, possible supply sag or phase imbalance"


def _band_earth_leak(earth_leak) -> str:
    e = float(earth_leak or 0)
    if e <= 1.0:
        return "normal earth leakage"
    if e <= 3.0:
        return "slightly elevated earth leakage"
    if e <= 6.0:
        return "elevated earth leakage approaching protection threshold"
    return "high earth leakage, ground protection fault likely"


def _band_temp(temp_c) -> str:
    t = float(temp_c or 0)
    if t <= 25:
        return "normal operating temperature"
    if t <= 45:
        return "warm, within expected range under load"
    if t <= 55:
        return "elevated temperature, possible cooling issue"
    if t <= 65:
        return "high temperature, thermal de-rating likely"
    return "critical temperature, thermal runaway risk"


def _band_fan(fan_rpm, power_w) -> str:
    rpm = float(fan_rpm or 0)
    power_w = float(power_w or 0)
    if rpm >= 1500:
        return "fan operating normally"
    if rpm >= 500:
        return "fan at reduced speed"
    if rpm > 0:
        return "fan critically degraded, bearing failure likely"
    if power_w > 0:
        return "fan not running under load, failure or blockage"
    return "fan idle, charger not under load"


def _band_errors(error_count) -> str:
    n = int(error_count or 0)
    if n == 0:
        return "no errors"
    if n <= 2:
        return "occasional errors"
    if n <= 5:
        return "frequent errors"
    return "error storm, multiple fault codes firing"


def _band_status(status_changes) -> str:
    s = int(status_changes or 0)
    if s <= 2:
        return "stable status"
    if s <= 5:
        return "some status transitions"
    if s <= 10:
        return "frequent status changes"
    return "status flapping, rapid state transitions"


def _band_severity(anomaly_score) -> str:
    score = float(anomaly_score or 0)
    if score == 0:
        return "no anomaly detected"
    if score <= 0.15:
        return "minor anomaly detected"
    if score <= 0.4:
        return "anomaly detected with elevated severity"
    if score <= 0.7:
        return "significant anomaly, investigation recommended"
    return "critical anomaly, immediate investigation required"


def build_window_text(row: dict) -> str:
    """Convert a charger_windows row into semantically banded text for embedding."""
    charger_id = row.get("charger_id", "unknown")
    power_w = row.get("avg_power_w") or row.get("max_power_w") or 0
    max_power_w = row.get("max_power_w", 0)
    status = row.get("charger_status", "")
    fan_rpm = row.get("avg_fan_rpm", 0)

    parts = [
        f"Charger {charger_id} five-minute window",
        _band_power(power_w, max_power_w, status).capitalize(),
        _band_earth_leak(row.get("max_earth_leak", 0)).capitalize(),
        _band_voltage(row.get("voltage_stddev", 0)).capitalize(),
        _band_temp(row.get("max_temp_c", 0)).capitalize(),
        _band_fan(fan_rpm, power_w).capitalize(),
    ]

    # Error description — include named codes if present
    error_band = _band_errors(row.get("error_count", 0))
    distinct_errors = row.get("distinct_errors")
    if distinct_errors:
        if isinstance(distinct_errors, str):
            try:
                distinct_errors = json.loads(distinct_errors)
            except (json.JSONDecodeError, ValueError):
                distinct_errors = [distinct_errors]
        if isinstance(distinct_errors, list) and distinct_errors:
            error_band += " including " + ", ".join(distinct_errors)
    parts.append(error_band.capitalize())

    parts.append(_band_status(row.get("status_changes", 0)).capitalize())

    # Anomaly flags
    flags = row.get("anomaly_flags")
    if flags:
        if isinstance(flags, str):
            try:
                flags = json.loads(flags)
            except (json.JSONDecodeError, ValueError):
                pass
        if isinstance(flags, list) and flags:
            parts.append("Flagged for " + ", ".join(flags))
        elif isinstance(flags, str) and flags:
            parts.append(f"Flagged for {flags}")

    parts.append(_band_severity(row.get("anomaly_score", 0)).capitalize())
    return ". ".join(parts) + "."


def build_outage_text(row: dict) -> str:
    """Convert an outage_catalog row into text for embedding."""
    symptoms = row.get("symptoms", "")
    if isinstance(symptoms, str):
        try:
            symptoms = json.loads(symptoms)
        except (json.JSONDecodeError, ValueError):
            pass
    if isinstance(symptoms, list):
        symptoms = ", ".join(str(s) for s in symptoms) + "."
    parts = [
        f"Pattern: {row.get('pattern_name', '')}",
        f"Category: {row.get('category', '')}",
        f"Severity: {row.get('severity', '')}",
        f"Root cause: {row.get('root_cause', '')}",
        f"Symptoms: {symptoms}",
        f"Resolution: {row.get('resolution', '')}",
    ]
    return ". ".join(parts)


def build_reasoning_text(row: dict) -> str:
    """Convert an agent_reasoning row into text for embedding."""
    text = row.get("observation", "")
    if row.get("hypothesis"):
        text += f" | Hypothesis: {row['hypothesis']}"
    if row.get("tags"):
        text += f" | Tags: {row['tags']}"
    return text


def build_memory_text(row: dict) -> str:
    """Convert a fleet_memory row into text for embedding."""
    return f"[{row.get('category', '')}] [{row.get('scope', '')}] {row.get('content', '')}"


TEXT_BUILDERS = {
    "charger_windows": build_window_text,
    "outage_catalog": build_outage_text,
    "agent_reasoning": build_reasoning_text,
    "fleet_memory": build_memory_text,
}


# ---------------------------------------------------------------------------
# TiDB writeback
# ---------------------------------------------------------------------------

def get_db():
    ssl_ca = os.environ.get("TIDB_SSL_CA")
    return pymysql.connect(
        **TIDB_CONFIG,
        ssl={"ca": ssl_ca} if ssl_ca else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def write_embeddings_batch(table: str, vec_column: str, id_column: str,
                           id_vec_pairs: list[tuple]) -> int:
    """Write a batch of embeddings back to TiDB."""
    if not id_vec_pairs:
        return 0

    db = get_db()
    try:
        with db.cursor() as cur:
            for row_id, vec in id_vec_pairs:
                cur.execute(
                    f"UPDATE {table} SET {vec_column} = %s WHERE {id_column} = %s",
                    (str(vec), row_id)
                )
        return len(id_vec_pairs)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Kafka consumer (TiCDC Canal-JSON format)
# ---------------------------------------------------------------------------

def parse_canal_json(message_value: bytes) -> Optional[dict]:
    """Parse a TiCDC Canal-JSON message. Returns the row data for INSERTs."""
    try:
        event = json.loads(message_value)
        # Canal-JSON format: {"type": "INSERT", "data": [{...}], "table": "..."}
        if event.get("type") in ("INSERT", "UPDATE"):
            rows = event.get("data", [])
            table = event.get("table", "")
            return {"table": table, "rows": rows}
    except (json.JSONDecodeError, KeyError):
        pass
    return None


def consume_and_embed():
    """Main consumer loop: read TiCDC events, batch embed, write back."""
    from kafka import KafkaConsumer

    topics = list(TOPIC_CONFIG.keys())
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKERS,
        group_id="ev-charger-embedding-service",
        auto_offset_reset="latest",
        value_deserializer=lambda v: v,  # raw bytes
        max_poll_records=EMBEDDING_BATCH_SIZE * 2,
        fetch_max_wait_ms=2000,
    )

    log.info(f"Consuming from topics: {topics}")
    log.info(f"Embedding provider: {EMBEDDING_PROVIDER}, batch size: {EMBEDDING_BATCH_SIZE}")

    # Batch accumulator: {table: [(row_id, text), ...]}
    batch = {}
    batch_count = 0
    last_flush = time.time()

    for message in consumer:
        event = parse_canal_json(message.value)
        if not event:
            continue

        table = event["table"]
        topic_cfg = None
        for cfg in TOPIC_CONFIG.values():
            if cfg["table"] == table:
                topic_cfg = cfg
                break
        if not topic_cfg:
            continue

        text_builder = TEXT_BUILDERS.get(table)
        if not text_builder:
            continue

        for row in event["rows"]:
            row_id = row.get(topic_cfg["id_column"])
            if not row_id:
                continue
            # Skip if vector already populated
            if row.get(topic_cfg["vec_column"]):
                continue

            text = text_builder(row)
            batch.setdefault(table, []).append((row_id, text, topic_cfg))
            batch_count += 1

        # Flush when batch is full or 5 seconds have passed
        if batch_count >= EMBEDDING_BATCH_SIZE or (time.time() - last_flush > 5.0):
            _flush_batch(batch)
            batch = {}
            batch_count = 0
            last_flush = time.time()


def _flush_batch(batch: dict):
    """Embed and write back a batch of rows."""
    for table, items in batch.items():
        if not items:
            continue

        texts = [text for _, text, _ in items]
        row_ids = [row_id for row_id, _, _ in items]
        cfg = items[0][2]

        try:
            log.info(f"Embedding {len(texts)} rows for {table}")
            embeddings = EMBED_FN(texts)

            pairs = list(zip(row_ids, embeddings))
            written = write_embeddings_batch(
                cfg["table"], cfg["vec_column"], cfg["id_column"], pairs
            )
            log.info(f"Wrote {written} embeddings to {table}.{cfg['vec_column']}")

        except Exception as e:
            log.error(f"Embedding failed for {table}: {e}")
            # TODO: Push to dead letter queue for retry


# ---------------------------------------------------------------------------
# Polling fallback (for environments without Kafka/TiCDC)
# ---------------------------------------------------------------------------

_POLL_TABLES = [
    ("charger_windows", {"vec_column": "signature_vec", "id_column": "id"}),
    ("outage_catalog",  {"vec_column": "signature_vec", "id_column": "id"}),
    ("agent_reasoning", {"vec_column": "reasoning_vec", "id_column": "id"}),
    ("fleet_memory",    {"vec_column": "memory_vec",    "id_column": "id"}),
]


def _embed_table_batch(db, table: str, cfg: dict, batch_size: int) -> int:
    """Embed one batch of NULL-vector rows for a table. Returns count written."""
    text_builder = TEXT_BUILDERS.get(table)
    if not text_builder:
        return 0

    with db.cursor() as cur:
        cur.execute(
            f"SELECT * FROM {table} "
            f"WHERE {cfg['vec_column']} IS NULL "
            f"ORDER BY {cfg['id_column']} ASC "
            f"LIMIT {batch_size}"
        )
        rows = cur.fetchall()

    if not rows:
        return 0

    texts = [text_builder(row) for row in rows]
    row_ids = [row[cfg["id_column"]] for row in rows]

    embeddings = EMBED_FN(texts)
    return write_embeddings_batch(table, cfg["vec_column"], cfg["id_column"],
                                  list(zip(row_ids, embeddings)))


def _reembed_table(table: str, cfg: dict, batch_size: int) -> int:
    """Re-embed all rows in a table using cursor-based pagination. Returns total written."""
    text_builder = TEXT_BUILDERS.get(table)
    if not text_builder:
        return 0

    id_col = cfg["id_column"]
    last_id = 0
    total = 0

    while True:
        db = get_db()
        try:
            with db.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {table} "
                    f"WHERE {id_col} > %s "
                    f"ORDER BY {id_col} ASC "
                    f"LIMIT {batch_size}",
                    (last_id,)
                )
                rows = cur.fetchall()
        finally:
            db.close()

        if not rows:
            break

        texts = [text_builder(row) for row in rows]
        row_ids = [row[id_col] for row in rows]
        last_id = max(row_ids)

        written = write_embeddings_batch(table, cfg["vec_column"], id_col,
                                         list(zip(row_ids, EMBED_FN(texts))))
        total += written
        log.info(f"{table}: +{written} this batch, {total} so far (cursor id>{last_id})")

    return total


def poll_and_embed(interval: int = 30, once: bool = False, batch_size: int = 32,
                   reembed: bool = False):
    """
    Fallback mode: poll TiDB directly for rows with NULL vector columns.
    Use this when TiCDC/Kafka is not available (e.g. dev/test).

    once=True: process all tables once and exit with a summary.
    once=False: loop forever, sleeping `interval` seconds between sweeps.
    reembed=True: regenerate ALL rows using cursor-based pagination (one-time
                  migration after text-builder format changes).
    """
    if reembed:
        log.info(f"Re-embed mode: paginating through all rows (batch_size={batch_size})")
        totals = {}
        for table, cfg in _POLL_TABLES:
            log.info(f"Starting re-embed for {table}...")
            totals[table] = _reembed_table(table, cfg, batch_size)
            log.info(f"{table}: complete ({totals[table]} rows embedded)")
        log.info("Re-embed complete. Summary:")
        for table, count in totals.items():
            log.info(f"  {table}: {count} rows embedded")
        return

    if once:
        log.info(f"One-shot backfill mode (batch_size={batch_size})")
        totals = {table: 0 for table, _ in _POLL_TABLES}

        while True:
            pass_total = 0
            for table, cfg in _POLL_TABLES:
                db = get_db()
                try:
                    written = _embed_table_batch(db, table, cfg, batch_size)
                finally:
                    db.close()

                if written == 0:
                    log.info(f"{table}: complete (embedded {totals[table]} rows total)")
                else:
                    totals[table] += written
                    pass_total += written
                    log.info(f"{table}: +{written} this batch, {totals[table]} so far")

            if pass_total == 0:
                break

        log.info("Backfill complete. Summary:")
        for table, count in totals.items():
            log.info(f"  {table}: {count} rows embedded")
        return

    log.info(f"Polling mode: checking for un-embedded rows every {interval}s "
             f"(batch_size={batch_size})")
    while True:
        db = get_db()
        try:
            for table, cfg in _POLL_TABLES:
                try:
                    written = _embed_table_batch(db, table, cfg, batch_size)
                    if written:
                        log.info(f"Wrote {written} embeddings to {table}")
                except Exception as e:
                    log.error(f"Embedding failed for {table}: {e}")
        finally:
            db.close()
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embedding service for EV charger platform")
    parser.add_argument("--poll", action="store_true",
                        help="Poll TiDB for NULL-vector rows (no Kafka/TiCDC required)")
    parser.add_argument("--once", action="store_true",
                        help="Single-pass backfill: exit when all NULL-vector rows are embedded "
                             "(requires --poll)")
    parser.add_argument("--batch-size", type=int, default=None,
                        help=f"Rows per embedding batch "
                             f"(default: EMBEDDING_BATCH_SIZE env var or {EMBEDDING_BATCH_SIZE})")
    parser.add_argument("--interval", type=int, default=30,
                        help="Seconds between sweeps in continuous --poll mode (default: 30)")
    parser.add_argument("--reembed", action="store_true",
                        help="Regenerate embeddings for ALL rows, not just NULL-vector rows. "
                             "Use after changing text builders to migrate existing embeddings. "
                             "Requires --poll.")
    args = parser.parse_args()

    if args.reembed and not args.poll:
        parser.error("--reembed requires --poll")

    effective_batch_size = args.batch_size if args.batch_size is not None else EMBEDDING_BATCH_SIZE

    if args.poll:
        poll_and_embed(interval=args.interval, once=args.once,
                       batch_size=effective_batch_size, reembed=args.reembed)
    else:
        consume_and_embed()
