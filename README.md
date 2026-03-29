# EV Charger Anomaly Detection Platform

A production-mimicking IoT platform for streaming telemetry from 20,000 OCPP electric vehicle chargers, with LLM-powered anomaly detection, vector similarity search, and a stateful agent memory architecture designed to minimise token cost at scale.

---

## Architecture

```
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
                                            HuggingFace: all-MiniLM-L6-v2
                                            (384-dim, local — no API key)
                                                       │
                                               UPDATE signature_vec
                                                       │
                                                       ▼
                                            ┌─────────────────────┐
                                            │   Claude Agent      │
                                            │   (tool use)        │
                                            │                     │
                                            │  search_similar_    │
                                            │    outages          │
                                            │  search_prior_      │
                                            │    diagnoses        │
                                            │  write_reasoning_   │
                                            │    checkpoint       │
                                            │  recall_fleet_      │
                                            │    memory           │
                                            │  write_fleet_memory │
                                            │  get_charger_context│
                                            │  get_recent_windows │
                                            │  get_session_state  │
                                            │  update_session_    │
                                            │    state            │
                                            └─────────────────────┘
```

**Development shortcut** — without Kafka/Flink:
```
stream_telemetry.py --format direct  →  TiDB (charger_telemetry + charger_windows)
embedding_service.py --poll          →  Poll for NULL vectors, embed locally
```

---

## Project Structure

```
ev-charger-platform/
├── schema.sql                     # Full TiDB DDL — data plane + context plane
├── tool_definitions.json          # Claude tool schemas (9 tools)
├── tool_handlers.py               # Tool handlers, context assembly, memory lifecycle
├── seed/
│   ├── seed_charger_registry.py   # 19,900 chargers across Ireland
│   ├── seed_outage_catalog.py     # 21 curated failure patterns (ground truth)
│   └── stream_telemetry.py        # OCPP telemetry simulator (direct-to-TiDB mode)
├── flink/
│   └── flink_windowing_job.py     # PyFlink: 5-min tumbling windows + anomaly scoring
├── embedding/
│   └── embedding_service.py       # Kafka/TiCDC consumer or poll mode embedder
└── config/
    └── ticdc_config.toml          # TiCDC changefeed configuration
```

---

## Memory Architecture

The agent uses a three-tier memory model designed to prevent token waste at scale.

### The Token Tax Problem
Without persistent memory, the agent re-derives the same conclusions on every invocation — re-examining the same charger data, re-forming the same hypotheses. At 20,000 chargers this is untenable.

### Three Tiers

| Tier | Store | Purpose | Token cost |
|---|---|---|---|
| **Working memory** | `session_state.investigation_summary` | In-progress reasoning, updated each step, max ~200 tokens | ~200 |
| **Outcome memory** | `agent_reasoning` | Confirmed/dismissed/escalated diagnoses only — no intermediate steps | ~300–500 |
| **Fleet knowledge** | `fleet_memory` | Patterns promoted from confirmed outcomes, scoped by model/site/global | ~100–300 |

### Key Design Decision: Ephemeral Reasoning

Intermediate reasoning is **not persisted**. The agent reasons freely in its context window and updates `investigation_summary` frequently. Only when an investigation reaches a conclusion (`confirmed`, `dismissed`, `escalated`) is a row written to `agent_reasoning` — **without an embedding**. The embedding is written back asynchronously by the TiCDC → embedding service pipeline, same as `charger_windows`. The vector is only needed for future similarity search, not at write time.

This means:
- No synchronous embedding API call in the agent's hot path
- `agent_reasoning` stays bounded — O(investigations resolved), not O(reasoning steps)
- `search_prior_diagnoses` only searches rows with populated vectors (`WHERE reasoning_vec IS NOT NULL`)

### Context Assembly Budget

`assemble_context` in `tool_handlers.py` builds the agent's system prompt under a 4,000-token hard cap:

| Priority | Source | Typical tokens |
|---|---|---|
| 1 | Charger profile (`context_snapshots`) | ~80 |
| 2 | Recent anomalies (`context_snapshots`) | ~100–300 |
| 3 | Active investigations (`agent_reasoning` open) | ~100–200 |
| 4 | Prior confirmed diagnoses (`search_prior_diagnoses`) | ~200–500 |
| 5 | Fleet knowledge (`recall_fleet_memory`) | ~100–300 |

Total: ~580–1,380 tokens — leaving 2,600+ for reasoning and response.

---

## Why Flink + TiCDC (not one or the other)?

**Flink** handles the hot ingest path: JSON parsing, 5-minute tumbling window aggregation, anomaly scoring. Stateful stream processing is what Flink is built for.

**TiCDC** handles the async embedding pipeline. When Flink writes a `charger_windows` row to TiDB, TiCDC captures the INSERT and pushes to Kafka, where the embedding service picks it up. This decouples the embedding call (50–200ms latency, rate-limited) from the Flink pipeline entirely — Flink writes at full speed, embeddings are generated asynchronously.

The same TiCDC pipeline handles `agent_reasoning` and `fleet_memory` — any new outcome or memory written by the agent is embedded without blocking the agent.

---

## Quick Start

### Prerequisites

```bash
pip install pymysql sentence-transformers langchain-community langchain-core python-dotenv
```

### 1. TiDB Cloud

Create a cluster at [tidbcloud.com](https://tidbcloud.com) with vector search enabled.

### 2. Environment

```bash
cp .env.example .env
# Fill in TIDB_HOST, TIDB_PORT, TIDB_USER, TIDB_PASSWORD, TIDB_DATABASE, TIDB_SSL_CA
```

`.env` values:
```env
TIDB_HOST=gateway01.<region>.prod.aws.tidbcloud.com
TIDB_PORT=4000
TIDB_USER=<cluster>.root
TIDB_PASSWORD=<password>
TIDB_DATABASE=ev_charger_platform
TIDB_SSL_MODE=VERIFY_IDENTITY
TIDB_SSL_CA=/path/to/isrgrootx1.pem

ANTHROPIC_API_KEY=sk-ant-...

# Embedding — local model, no API key needed
EMBEDDING_PROVIDER=huggingface
EMBEDDING_MODEL=all-MiniLM-L6-v2
EMBEDDING_DIM=384
```

### 3. Apply Schema

```bash
set -a && source .env && set +a

mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-mode="$TIDB_SSL_MODE" --ssl-ca="$TIDB_SSL_CA" \
  "$TIDB_DATABASE" < schema.sql
```

### 4. Seed Reference Data

```bash
cd seed

# 19,900 chargers across Ireland
python3 seed_charger_registry.py --format sql | \
  mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-mode="$TIDB_SSL_MODE" --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE"

# 21 curated failure patterns
python3 seed_outage_catalog.py | \
  mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-mode="$TIDB_SSL_MODE" --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE"
```

### 5. Embed Outage Catalog

Downloads `all-MiniLM-L6-v2` (~90MB) on first run. One-shot backfill:

```bash
set -a && source .env && set +a
python3 embedding/embedding_service.py --poll --once
```

### 6. Stream Telemetry (Development — No Kafka/Flink)

Writes directly to TiDB, computing window aggregates in Python:

```bash
set -a && source .env && set +a

# 200 chargers, 100x speed, 1 hour of data
python3 seed/stream_telemetry.py \
  --chargers 200 --speed 100 --duration 3600 --format direct
```

Then run the embedding service in continuous poll mode to embed the new windows:

```bash
python3 embedding/embedding_service.py --poll
```

### 7. Run the Agent

**CLI runner** — `agent/run_agent.py`:

```bash
# Investigate a specific charger with an explicit trigger
python3 agent/run_agent.py --charger CP-IE-TEST-00042 \
  --trigger "Anomaly score 0.73, voltage stddev 12.3V, earth leakage 7.2mA"

# Auto-build trigger from the charger's most recent anomalous window
python3 agent/run_agent.py --charger CP-IE-TEST-00042

# Find the highest-scoring charger in the last 24h and investigate it
python3 agent/run_agent.py --auto
```

**Python API**:

```python
from dotenv import load_dotenv
load_dotenv()

from tool_handlers import run_agent

result = run_agent(
    session_id="sess-001",
    user_id="engineer-01",
    charger_id="CP-IE-DUB-00042",
    trigger=(
        "Charger CP-IE-DUB-00042 has anomaly score 0.73. "
        "Voltage stddev 12.3V, earth leakage 7.2mA, 4 errors in last window."
    ),
)
print(result["response"])
```

---

## Production Deployment (Kafka + Flink + TiCDC)

### Flink

```bash
export KAFKA_BROKERS=kafka:9092
export TIDB_HOST=gateway01.xxx.tidbcloud.com
export TIDB_PORT=4000
export TIDB_DATABASE=ev_charger_platform
export TIDB_USER=root
export TIDB_PASSWORD=xxx

flink run -py flink/flink_windowing_job.py
```

### TiCDC

```bash
# Update config/ticdc_config.toml database name to match your cluster
cdc cli changefeed create \
  --sink-uri="kafka://kafka:9092/ticdc-charger-windows?protocol=canal-json" \
  --config config/ticdc_config.toml
```

### Embedding Service (Kafka mode)

```bash
export KAFKA_BROKERS=kafka:9092
python3 embedding/embedding_service.py
```

> For production workloads, swap `EMBEDDING_MODEL` to a hosted API (Voyage AI, OpenAI `text-embedding-3-small`) and update `EMBEDDING_PROVIDER`. The rest of the pipeline is unchanged.

---

## Database Schema

Two planes in a single TiDB Cloud cluster:

**Data plane** — IoT telemetry
| Table | Purpose |
|---|---|
| `charger_registry` | Static charger metadata (19,900 records) |
| `charger_telemetry` | Raw OCPP messages (Flink sink) |
| `charger_windows` | 5-min aggregates with anomaly scores + 384-dim HNSW vector |
| `outage_catalog` | 21 curated failure patterns + 384-dim HNSW vector |

**Context plane** — Agent memory
| Table | Purpose |
|---|---|
| `agent_reasoning` | Outcome records only (confirmed/dismissed/escalated diagnoses) |
| `fleet_memory` | Promoted cross-fleet patterns, deduplicated at cosine distance < 0.15 |
| `session_state` | Active investigation state + `investigation_summary` working memory |
| `context_snapshots` | Cached, token-counted prompt fragments |

All vector columns: `VECTOR(384)`, HNSW cosine index.

---

## Related

- [tidb-self-healing-db-agent](https://github.com/bernard-kavanagh/tidb-self-healing-db-agent) — same embedding stack (`all-MiniLM-L6-v2`, `TiDBVectorStore`), applied to autonomous database repair
