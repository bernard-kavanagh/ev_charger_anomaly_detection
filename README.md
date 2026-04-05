# EV Charger IoT Platform

Single-cluster TiDB Cloud architecture for streaming IoT telemetry from 20,000 OCPP chargers with self-improving AI diagnostic agents.

**v2 (April 2026):** Hybrid search, contradiction resolution, fleet memory compaction, anomaly explainability, data validation, structured observability, and 36 unit tests. See [UPGRADE.md](UPGRADE.md) for the full changelog.

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
              (anomaly scoring + breakdown)            │
                                               TiCDC captures INSERT
                                                       │
                                                       ▼
                                            Kafka (ticdc-charger-windows)
                                                       │
                                                       ▼
                                            Embedding Service
                                            (HuggingFace all-MiniLM-L6-v2)
                                            + text_bander.py (shared banding)
                                            UPDATE signature_vec
                                                       │
                                                       ▼
                                            ┌─────────────────────────┐
                                            │     Claude Agent        │
                                            │  run_agent.py --auto    │
                                            │  dispatch.py --top 5    │
                                            │  + hybrid search        │
                                            │  + circuit breaker      │
                                            │  + observability        │
                                            └─────────────────────────┘

                        DEVELOPMENT PATH (no Kafka/Flink required)
                        ──────────────────────────────────────────
stream_telemetry.py --format direct ────────► TiDB: charger_telemetry
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

| Package | Required for |
|---|---|
| `pymysql` | TiDB connection (all components) |
| `python-dotenv` | `.env` loading (all components) |
| `anthropic` | Claude API — agent loop |
| `tiktoken` | Token counting for context budget |
| `sentence-transformers` | HuggingFace embedding model |
| `langchain-community` | `HuggingFaceEmbeddings` wrapper used by embedding service |
| `pytest` | Unit test suite |
| `kafka-python` | Kafka consumer — production TiCDC mode only |
| `pyflink` | Flink windowing job — production only |

---

## Quick Start

### 1. Clone and configure

```bash
git clone <repo-url>
cd ev-charger-platform
cp .env.example .env
# Edit .env with your TiDB Cloud and Anthropic credentials
```

### 2. Apply schema

```bash
set -a && source .env && set +a

# Base schema (8 tables)
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" < schema.sql

# v2 migration (FULLTEXT indexes, anomaly_breakdown, contradiction tracking)
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" < schema_v2.sql
```

### 3. Seed reference data

```bash
set -a && source .env && set +a

# 20,000 charger records across 35 Irish sites
python3 seed/seed_charger_registry.py

# 24 curated outage patterns (ground truth for vector search)
python3 seed/seed_outage_catalog.py
```

### 4. Run tests

```bash
python -m pytest tests/ -v
# Expected: 36 passed
```

### 5. Generate telemetry and embed

```bash
# Simulate 200 chargers, 30 minutes of data at 100x speed
python3 seed/stream_telemetry.py --chargers 200 --duration 1800 --speed 100 --format direct

# Embed all charger_windows and outage_catalog rows with NULL vectors
python3 embedding/embedding_service.py --poll --once --batch-size 100
```

### 6. Run the agent

```bash
# Auto-select the charger with the highest anomaly score in the last 24h
python3 agent/run_agent.py --auto

# Investigate a specific charger
python3 agent/run_agent.py --charger CP-IE-TEST-00042

# Run 5 concurrent investigations against the top-5 anomalous chargers
python3 agent/dispatch.py --top 5
```

---

## Usage Reference

### Telemetry Simulator

```bash
# 200 chargers, 30 minutes, written directly to TiDB
python3 seed/stream_telemetry.py --chargers 200 --duration 1800 --speed 100 --format direct

# All 20,000 chargers, 1 hour, slower for lower DB load
python3 seed/stream_telemetry.py --chargers 20000 --duration 3600 --speed 50 --format direct
```

### Embedding Service

```bash
# One-shot backfill: embed all rows with NULL vector columns, then exit
python3 embedding/embedding_service.py --poll --once

# Re-embed everything after a text-builder change
python3 embedding/embedding_service.py --poll --once --reembed --batch-size 100

# Continuous poll
python3 embedding/embedding_service.py --poll --interval 30
```

### Single-Agent CLI

```bash
python3 agent/run_agent.py --auto
python3 agent/run_agent.py --charger CP-IE-TEST-00042
python3 agent/run_agent.py --charger CP-IE-TEST-00042 \
  --trigger "Anomaly score 0.73, earth leakage 7.2mA, 11 errors in window"
```

### Multi-Agent Dispatcher

```bash
python3 agent/dispatch.py                    # top 5 (default)
python3 agent/dispatch.py --top 10 --workers 3
python3 agent/dispatch.py --chargers CP-IE-TEST-00042,CP-IE-TEST-00072
python3 agent/dispatch.py --top 10 --dry-run
```

### Lifecycle Jobs

```bash
# Promote confirmed reasoning patterns into fleet memory (run daily)
python3 -c "from tool_handlers import consolidation_job; print(consolidation_job())"

# Expire sessions, prune snapshots, decay confidence, deprecate stale memories
python3 -c "from tool_handlers import cleanup_job; print(cleanup_job())"

# Re-cluster and merge drifted fleet memories (run weekly)
python3 -c "from tool_handlers import compaction_job; print(compaction_job())"
```

---

## Project Structure

```
ev-charger-platform/
├── .env                          # TiDB + Anthropic credentials (not committed)
├── schema.sql                    # TiDB DDL (8 tables, v2 inline: FULLTEXT, anomaly_breakdown, superseded_by)
├── schema_v2.sql                 # Stub (merged into schema.sql)
├── reset.sql                     # Drop all tables (for clean reset)
├── requirements.txt              # Python dependencies
├── tool_definitions.json         # Claude tool schemas (9 tools)
├── tool_handlers.py              # Tool handlers, context assembly, lifecycle jobs (v2: hybrid search, circuit breaker, @_safe_handler)
├── text_bander.py                # Shared semantic banding module (v2)
├── validation.py                 # Data quality checks (v2)
├── observability.py              # Structured JSON logging via AgentObserver (v2)
├── UPGRADE.md                    # v2 changelog
├── check-upgrade.md              # Claude CLI audit prompt
├── seed/
│   ├── seed_charger_registry.py  # Generate 20,000 charger records
│   ├── seed_outage_catalog.py    # 24 curated failure patterns
│   └── stream_telemetry.py       # Telemetry simulator with --direct mode
├── embedding/
│   └── embedding_service.py      # Embedding pipeline (poll/Kafka/reembed modes, imports text_bander)
├── agent/
│   ├── __init__.py               # Package marker
│   ├── run_agent.py              # Single-agent CLI
│   └── dispatch.py               # Multi-agent concurrent dispatcher
├── flink/
│   └── flink_windowing_job.py    # PyFlink job (optional, for production)
├── config/
│   └── ticdc_config.toml         # TiCDC changefeed config (Essentials/Dedicated)
└── tests/
    ├── __init__.py               # Test package marker
    └── test_tool_handlers.py     # 36 unit tests (v2)
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
| `charger_windows` | 5-min aggregates with anomaly scores, breakdown, and 384-dim vector |
| `outage_catalog` | 24 curated failure patterns with 384-dim vector + FULLTEXT index |

**Context plane** — Agent memory

| Table | Purpose |
|---|---|
| `session_state` | Active investigation state per session |
| `agent_reasoning` | Outcome records with supersession tracking + FULLTEXT index |
| `fleet_memory` | Promoted cross-fleet patterns, cosine-deduplicated + FULLTEXT index |
| `context_snapshots` | Cached, token-counted prompt fragments |

All vector columns: `VECTOR(384)`, HNSW cosine index.

### The 9 tools

| Tool | What it does |
|---|---|
| `get_charger_context` | Fetch charger profile from registry |
| `get_recent_windows` | Retrieve recent anomaly windows with breakdown |
| `search_similar_outages` | Hybrid search: vector + FULLTEXT against outage catalog |
| `search_prior_diagnoses` | Vector search against agent_reasoning (excludes superseded) |
| `write_reasoning_checkpoint` | Persist conclusion + auto-supersede prior diagnoses |
| `recall_fleet_memory` | Single-query hybrid search with scope ranking |
| `write_fleet_memory` | Write/merge/supersede fleet knowledge |
| `get_session_state` | Read current session investigation state |
| `update_session_state` | Write updated focus chargers and summary |

### Context assembly and token budget

`assemble_context()` in `tool_handlers.py` builds the agent's system prompt under a 4,000-token hard cap (3,600 effective after 10% safety margin), loading sources in priority order:

| Priority | Source | Typical tokens |
|---|---|---|
| 1 | Charger profile (registry + snapshots) | ~80 |
| 2 | Recent anomaly windows + breakdown | ~100–300 |
| 3 | Active investigations (non-superseded) | ~100–200 |
| 4 | Prior confirmed diagnoses (hybrid search) | ~200–500 |
| 5 | Fleet knowledge (single-query scoped recall) | ~100–300 |

Total: ~580–1,380 tokens, leaving 2,200+ for reasoning and response.

### Memory lifecycle

1. **Write control:** Agent writes a reasoning checkpoint only when it reaches a conclusion (confirmed/dismissed/escalated). Intermediate reasoning stays ephemeral.
2. **Deduplication:** `write_fleet_memory()` checks cosine distance < 0.15 before insert. Near-duplicates are merged, incrementing `supporting_evidence_count`.
3. **Contradiction resolution:** Distance 0.15–0.40 in the same category and scope triggers auto-supersession via `superseded_by`. Prior diagnoses for the same charger are linked.
4. **Confidence decay:** `cleanup_job()` applies `confidence *= 0.95` monthly for memories older than 30 days. Memories below 0.30 are auto-deprecated.
5. **Compaction:** `compaction_job()` weekly re-clusters fleet memories, merging entries that have drifted within cosine distance 0.20.
6. **Forgetting:** TTL policies cap storage at ~960M rows. 90-day zero-access deprecation. Expired context snapshots pruned automatically.

### Hybrid search

`_hybrid_search()` combines vector cosine distance with FULLTEXT keyword matching in a single query. `_extract_keywords()` identifies fault codes (E-001, GroundFailure), model names (Terra 54), firmware versions (3.1.2), and environment types. Falls back to vector-only if FULLTEXT indexes aren't present.

### Anomaly explainability

The `anomaly_breakdown` JSON column on `charger_windows` stores per-feature scores (`voltage_instability`, `thermal_stress`, `error_rate`, `earth_leakage`, `status_flapping`). The agent includes this breakdown in diagnostic reports so operators can see *why* a score was assigned.

### Observability

`AgentObserver` in `observability.py` emits structured JSON logs for tool call latencies, context assembly time, vector search distances, circuit breaker triggers, and per-agent summaries. The agent loop wires it in automatically.

### Safety

- **Circuit breaker:** `max_tool_rounds=15` + halt if the same tool is called 3x consecutively.
- **Error handling:** `@_safe_handler` decorator on all tool handlers returns structured error JSON instead of crashing.
- **Data validation:** `validation.py` catches physically impossible telemetry values before they enter the data plane.

---

## v2 Upgrade (April 2026)

The platform underwent a 25-point architecture audit. Result: 22 green, 3 orange, 0 red. 36 unit tests passing.

**New capabilities:**
- **Hybrid search:** Vector cosine + FULLTEXT keyword matching in a single SQL query
- **Contradiction resolution:** `superseded_by` column auto-links stale conclusions to newer evidence
- **Fleet memory compaction:** Weekly re-clustering merges drifted memories (cosine < 0.20)
- **Confidence decay:** 5% monthly decay without reinforcement; auto-deprecated below 0.30
- **Anomaly explainability:** Per-feature score breakdown in `anomaly_breakdown` JSON column
- **Data validation:** Reject physically impossible telemetry before ingest
- **Observability:** Structured JSON logging via `AgentObserver` class
- **Circuit breaker:** `max_tool_rounds=15`, consecutive-same-tool detection
- **Error handling:** `@_safe_handler` decorator on all 9 tool handlers
- **Token safety margin:** 10% buffer (3,600 effective from 4,000 nominal)
- **Shared text banding:** `text_bander.py` used by both embedding service and agent context assembly
- **Single-query scoped recall:** 3 scope queries collapsed to 1 with post-ranking

See `UPGRADE.md` for the full changelog.

---

## Key Design Decisions

- **Single cluster, unified platform.** TiDB's native `VECTOR` type with HNSW indexing handles both OLTP reads/writes and vector similarity search. No frankenstack of bolt-on services — every additional system boundary adds sync lag, consistency gaps, and token debt.
- **Polling mode is first-class for Serverless.** `--poll --once` is the intended embedding path when TiCDC changefeeds are unavailable.
- **Outcomes only in agent_reasoning.** Intermediate reasoning is not persisted. Only conclusions are written. Table bounded at O(investigations) not O(reasoning steps).
- **Semantic banding for embeddings.** `text_bander.py` converts raw metrics to natural language ("slightly elevated earth leakage", "error storm"). This is the single source of truth — both `embedding_service.py` and `stream_telemetry.py` import from the same module.
- **384-dim all-MiniLM-L6-v2.** Runs locally, no API key, no rate limits, no per-token cost.
- **Memory is infrastructure, not a feature.** Write control, deduplication, reconciliation, decay, compaction, and forgetting are all implemented as explicit lifecycle mechanisms — not bolted-on afterthoughts.

---

## TiDB Cloud Tier Guide

| Feature | Serverless | Essentials | Dedicated |
|---|---|---|---|
| Vector search (`VECTOR`, HNSW) | Yes | Yes | Yes |
| FULLTEXT indexes (hybrid search) | Yes | Yes | Yes |
| TiCDC changefeeds | No | Yes | Yes |
| Kafka sink | No | Yes | Yes |
| TTL policies | Yes | Yes | Yes |
| Recommended embedding mode | `--poll` | TiCDC + Kafka | TiCDC + Kafka |

---

## Test Suite

```bash
python -m pytest tests/ -v
```

36 tests across 7 classes:

| Class | Tests | Covers |
|---|---|---|
| `TestTiDBEncoder` | 3 | Decimal/datetime JSON serialisation |
| `TestTokenBudget` | 4 | Safety margin (10%), token counting |
| `TestKeywordExtraction` | 6 | Hybrid search keyword extraction |
| `TestTextBanding` | 9 | All banding functions (power, voltage, temp, leak, fan, errors, status) |
| `TestAnomalyBreakdown` | 5 | Per-feature scoring, breakdown explainability |
| `TestValidation` | 9 | Telemetry and window validation edge cases |

No live database connection required.
