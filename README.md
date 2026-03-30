# EV Charger IoT Platform

Single-cluster TiDB Cloud architecture for streaming IoT telemetry from 20,000 OCPP chargers with self-improving AI diagnostic agents.

---

## Architecture

```
                        PRODUCTION PATH
                        ───────────────
OCPP Chargers (20,000)
    │ WebSocket / OCPP-J
    ▼
Kafka (ocpp-telemetry)
    │
    ├──► Flink: raw pass-through ──────────────► TiDB: charger_telemetry
    │
    └──► Flink: 5-min tumbling windows ────────► TiDB: charger_windows
              (anomaly scoring)                        │
                                               TiCDC captures INSERT
                                                       │
                                                       ▼
                                            Kafka (ticdc-charger-windows)
                                                       │
                                                       ▼
                                            Embedding Service
                                            (HuggingFace all-MiniLM-L6-v2)
                                            UPDATE signature_vec
                                                       │
                                                       ▼
                                            ┌─────────────────────────┐
                                            │     Claude Agent        │
                                            │  run_agent.py --auto    │
                                            │  dispatch.py --top 5    │
                                            └─────────────────────────┘

                        DEVELOPMENT PATH (no Kafka/Flink required)
                        ──────────────────────────────────────────
stream_telemetry.py --direct  ──────────────► TiDB: charger_telemetry
                                                    charger_windows
                                                       │
                              embedding_service.py --poll --once
                                                       │
                                                       ▼
                                            ┌─────────────────────────┐
                                            │     Claude Agent        │
                                            └─────────────────────────┘
```

---

## Prerequisites

- Python 3.10+
- TiDB Cloud cluster — Serverless for development, Essentials or Dedicated for production (TiCDC requires Essentials+)
- Anthropic API key

---

## Python Dependencies

```bash
pip install -r requirements.txt
```

Or directly:

```bash
pip install pymysql python-dotenv anthropic tiktoken sentence-transformers langchain-community
```

| Package | Required for |
|---|---|
| `pymysql` | TiDB connection (all components) |
| `python-dotenv` | `.env` loading (all components) |
| `anthropic` | Claude API — agent loop |
| `tiktoken` | Token counting for context budget |
| `sentence-transformers` | HuggingFace embedding model |
| `langchain-community` | `HuggingFaceEmbeddings` wrapper used by embedding service |
| `kafka-python` | Kafka consumer — production TiCDC mode only |
| `pyflink` | Flink windowing job — production only |

> `langchain-community` shows a deprecation warning for `HuggingFaceEmbeddings`. The replacement is `langchain-huggingface`, but `langchain-community` works fine and requires fewer transitive dependencies.

---

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd ev-charger-platform
cp .env.example .env
```

Edit `.env`:

```env
TIDB_HOST=gateway01.<region>.prod.aws.tidbcloud.com
TIDB_PORT=4000
TIDB_USER=<cluster>.root
TIDB_PASSWORD=<password>
TIDB_DATABASE=ev_charger
TIDB_SSL_CA=/path/to/isrgrootx1.pem   # download from TiDB Cloud console

ANTHROPIC_API_KEY=sk-ant-...

EMBEDDING_PROVIDER=huggingface
EMBEDDING_MODEL=all-MiniLM-L6-v2
EMBEDDING_DIM=384
```

### 2. Apply schema

```bash
set -a && source .env && set +a

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" < schema.sql
```

### 3. Seed reference data

```bash
set -a && source .env && set +a

# 20,000 charger records across 35 Irish sites
python3 seed/seed_charger_registry.py

# 24 curated outage patterns (ground truth for vector search)
python3 seed/seed_outage_catalog.py
```

### 4. Generate telemetry and embed

```bash
# Simulate 200 chargers, 30 minutes of data at 100x speed
# --direct writes straight to TiDB, no Kafka/Flink needed
python3 seed/stream_telemetry.py --chargers 200 --duration 1800 --speed 100 --direct

# Embed all charger_windows and outage_catalog rows with NULL vectors
# Downloads all-MiniLM-L6-v2 (~90MB) on first run
python3 embedding/embedding_service.py --poll --once --batch-size 100
```

### 5. Run the agent

```bash
# Auto-select the charger with the highest anomaly score in the last 24h
python3 agent/run_agent.py --auto

# Investigate a specific charger (builds trigger from its most recent anomalous window)
python3 agent/run_agent.py --charger CP-IE-TEST-00042

# Run 5 concurrent investigations against the top-5 anomalous chargers
python3 agent/dispatch.py --top 5
```

---

## Usage Reference

### Telemetry Simulator

```bash
# 200 chargers, 30 minutes of simulated data, written directly to TiDB
python3 seed/stream_telemetry.py --chargers 200 --duration 1800 --speed 100 --direct

# All 20,000 chargers, 1 hour, slower for lower DB load
python3 seed/stream_telemetry.py --chargers 20000 --duration 3600 --speed 50 --direct
```

### Embedding Service

```bash
# One-shot backfill: embed all rows with NULL vector columns, then exit
python3 embedding/embedding_service.py --poll --once

# Re-embed everything after a text-builder change (cursor-paginates through all rows)
python3 embedding/embedding_service.py --poll --once --reembed --batch-size 100

# Continuous poll: check for new NULL-vector rows every 30 seconds
python3 embedding/embedding_service.py --poll --interval 30
```

### Single-Agent CLI

```bash
# Auto-select highest anomaly score in last 24h
python3 agent/run_agent.py --auto

# Investigate a specific charger (derives trigger from its most recent anomalous window)
python3 agent/run_agent.py --charger CP-IE-TEST-00042

# Explicit trigger text
python3 agent/run_agent.py --charger CP-IE-TEST-00042 \
  --trigger "Anomaly score 0.73, earth leakage 7.2mA, 11 errors in window"
```

### Multi-Agent Dispatcher

```bash
# Investigate top 5 most anomalous chargers concurrently (default)
python3 agent/dispatch.py

# Investigate top 10 with concurrency capped at 3 workers
python3 agent/dispatch.py --top 10 --workers 3

# Investigate specific chargers
python3 agent/dispatch.py --chargers CP-IE-TEST-00042,CP-IE-TEST-00072,CP-IE-TEST-00115

# Show the dispatch plan without running agents
python3 agent/dispatch.py --top 10 --dry-run
```

Dispatcher output includes a fleet summary with total tool calls, reasoning checkpoints, fleet memory writes, wall-clock time, per-agent durations, and a concurrency speedup ratio.

---

## TiDB Cloud Tier Guide

| Feature | Serverless | Essentials | Dedicated |
|---|---|---|---|
| Vector search (`VECTOR`, HNSW) | Yes | Yes | Yes |
| TiCDC changefeeds | No | Yes | Yes |
| Kafka sink | No | Yes | Yes |
| TTL policies | Yes | Yes | Yes |
| Recommended embedding mode | `--poll` | TiCDC + Kafka | TiCDC + Kafka |
| When to use | Development, demos | Production | High-throughput production |

The development path (`--direct` + `--poll`) is **first-class**, not a fallback. For demos and development, it is simpler and faster to set up than the full Kafka/Flink pipeline.

---

## Production Deployment

### Flink windowing job

```bash
export KAFKA_BROKERS=kafka:9092
export TIDB_HOST=gateway01.xxx.tidbcloud.com
export TIDB_PORT=4000
export TIDB_DATABASE=ev_charger
export TIDB_USER=root
export TIDB_PASSWORD=xxx

flink run -py flink/flink_windowing_job.py
```

### TiCDC changefeed

```bash
# Update config/ticdc_config.toml with your database name, then:
cdc cli changefeed create \
  --sink-uri="kafka://kafka:9092/ticdc-charger-windows?protocol=canal-json" \
  --config config/ticdc_config.toml
```

### Embedding service (Kafka/TiCDC mode)

```bash
export KAFKA_BROKERS=kafka:9092
python3 embedding/embedding_service.py
```

### TTL retention policies

TTL `ALTER TABLE` statements are documented as commented-out blocks at the bottom of `schema.sql`. Execute them to cap steady-state storage at ~960M rows (vs unbounded growth). Do not enable during demos — TTL will delete simulated data once the retention period expires.

---

## Project Structure

```
ev-charger-platform/
├── .env                          # TiDB + Anthropic credentials (not committed)
├── schema.sql                    # TiDB DDL (8 tables + TTL policies)
├── reset.sql                     # Drop all tables (for clean reset)
├── requirements.txt              # Python dependencies
├── tool_definitions.json         # Claude tool schemas (9 tools)
├── tool_handlers.py              # Tool handlers, context assembly, agent loop
├── seed/
│   ├── seed_charger_registry.py  # Generate 20,000 charger records
│   ├── seed_outage_catalog.py    # 24 curated failure patterns
│   └── stream_telemetry.py       # Telemetry simulator with --direct mode
├── embedding/
│   └── embedding_service.py      # Embedding pipeline (poll/Kafka/reembed modes)
├── agent/
│   ├── __init__.py               # Package marker
│   ├── run_agent.py              # Single-agent CLI
│   └── dispatch.py               # Multi-agent concurrent dispatcher
├── flink/
│   └── flink_windowing_job.py    # PyFlink job (optional, for production)
└── config/
    └── ticdc_config.toml         # TiCDC changefeed config (Essentials/Dedicated)
```

---

## How It Works

### Data plane vs context plane

All state lives in a single TiDB Cloud cluster split across two logical planes:

**Data plane** — IoT telemetry

| Table | Purpose |
|---|---|
| `charger_registry` | Static metadata for 20,000 chargers across 35 sites |
| `charger_telemetry` | Raw OCPP messages |
| `charger_windows` | 5-min aggregates with anomaly scores and 384-dim vector |
| `outage_catalog` | 24 curated failure patterns with 384-dim vector |

**Context plane** — Agent memory

| Table | Purpose |
|---|---|
| `session_state` | Active investigation state per session |
| `agent_reasoning` | Outcome records (confirmed / dismissed / escalated) |
| `fleet_memory` | Promoted cross-fleet patterns, cosine-deduplicated at < 0.15 |
| `context_snapshots` | Cached, token-counted prompt fragments |

All vector columns: `VECTOR(384)`, HNSW cosine index.

### The 9 tools

| Tool | What it does |
|---|---|
| `get_charger_context` | Fetch charger profile from registry |
| `get_recent_windows` | Retrieve recent anomaly windows for a charger |
| `search_similar_outages` | Vector search against outage catalog |
| `search_prior_diagnoses` | Vector search against agent_reasoning |
| `write_reasoning_checkpoint` | Persist in-progress hypothesis to agent_reasoning |
| `recall_fleet_memory` | Vector search against fleet_memory |
| `write_fleet_memory` | Promote a confirmed pattern to fleet_memory |
| `get_session_state` | Read current session investigation state |
| `update_session_state` | Write updated focus chargers and summary |

### Context assembly and token budget

`assemble_context()` in `tool_handlers.py` builds the agent's system prompt under a 4,000-token hard cap, loading sources in priority order:

| Priority | Source | Typical tokens |
|---|---|---|
| 1 | Charger profile (registry + snapshots) | ~80 |
| 2 | Recent anomaly windows | ~100–300 |
| 3 | Active investigations (open agent_reasoning) | ~100–200 |
| 4 | Prior confirmed diagnoses (vector search) | ~200–500 |
| 5 | Fleet knowledge (vector search) | ~100–300 |

Total: ~580–1,380 tokens, leaving 2,600+ for reasoning and response.

### Memory lifecycle

1. Agent writes a **reasoning checkpoint** (`write_reasoning_checkpoint`) when it reaches a hypothesis — persisted to `agent_reasoning`.
2. When the investigation concludes, the agent calls `write_fleet_memory` to promote confirmed patterns — deduplicated by cosine distance against existing memories.
3. Future agents on the same or similar chargers load relevant fleet memories via `recall_fleet_memory` before forming their own hypotheses.
4. The TiCDC pipeline (or `--poll` mode) asynchronously embeds `agent_reasoning` rows so they become searchable via `search_prior_diagnoses`.

---

## Key Design Decisions

- **Single cluster, no separate vector store.** TiDB's native `VECTOR` type with HNSW indexing handles both OLTP reads/writes and vector similarity search. No Pinecone, Weaviate, or pgvector sidecar needed.
- **Polling mode is first-class for Serverless.** `--poll --once` is not a workaround — it is the intended embedding path when TiCDC changefeeds are unavailable. The development experience is identical to production except for the embedding trigger mechanism.
- **Outcomes only in agent_reasoning.** Intermediate reasoning is not persisted. Only confirmed, dismissed, or escalated diagnoses are written. This keeps the table bounded at O(investigations) rather than O(reasoning steps).
- **Semantic banding for embeddings.** `charger_windows` rows are embedded as descriptive natural language ("slightly elevated earth leakage", "error storm, multiple fault codes firing") rather than raw key=value pairs. This aligns the embedding vocabulary with the outage catalog's symptom descriptions and improves vector recall.
- **384-dim all-MiniLM-L6-v2.** Runs locally with no API key, no rate limits, no per-token cost. Matches the `VECTOR(384)` schema columns. Swappable for a hosted model by changing `EMBEDDING_PROVIDER` and the embedding function — the rest of the pipeline is unchanged.
- **Concurrent OLTP showcase.** The dispatcher runs N agents simultaneously against the same TiDB cluster. Each agent has its own `session_id` and makes independent reads and writes to `agent_reasoning`, `fleet_memory`, and `session_state`. Fleet memory dedup is best-effort under concurrency by design — this demonstrates TiDB's concurrent OLTP capability without artificial serialisation.
