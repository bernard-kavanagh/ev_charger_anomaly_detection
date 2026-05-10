# EV Charger IoT Platform

Single-cluster TiDB Cloud architecture for streaming IoT telemetry from 20,000 OCPP chargers with self-improving AI diagnostic agents.

This platform implements the **cognitive foundation** architecture — a three-tier memory structure (episodic, semantic, procedural) with five custodial duties, running on a unified data substrate. It solves the **Memory Wall**: the infrastructure problem caused by stateless models on fragmented stacks.

**v3 (May 2026):** Routing layer (Haiku shortcut path), Tier 5 graceful degradation, hard token caps on seed assembly, slim summary call, system prompt caching, charger registry write-on-create. See [AGENT_LIFECYCLE.md](AGENT_LIFECYCLE.md) for the full agent flow, empirical results from a multi-cluster experiment, and the reframed cost/quality thesis.

**v2 (April 2026):** Hybrid search, contradiction resolution, fleet memory compaction, anomaly explainability, data validation, structured observability, and 36 unit tests. See [UPGRADE.md](UPGRADE.md) for the full changelog.

## Validated thesis (post-experiment)

The cognitive foundation provides four measurable benefits:

1. **Persistent fleet awareness** — cluster recognition, recurrence detection, and cross-charger evidence chains that stateless agents cannot produce. Universal across all cluster states tested.
2. **Higher-confidence diagnoses** — warm investigations run at 0.93–0.99 confidence vs 0.72–0.78 cold. Same model, same telemetry, stronger conclusions when validating priors are available.
3. **~75% dollar-cost reduction** at steady state, via routing high-confidence pattern matches to a Haiku shortcut path. Mechanism: model substitution (Sonnet → Haiku), not pure token reduction.
4. **~40% wall-clock latency reduction** on the shortcut path. Operator-facing.

Token reductions vary by fleet state (~31% on 3-week production fleets, ~57% on smaller warmed clusters). The dollar saving is dominated by the model swap and holds across all states tested. Cold clusters need a 15-25 dispatch warm-up before shortcuts fire reliably; production clusters with accumulated memory route 100% shortcut from the first dispatch.

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
                                            │  + routing layer        │
                                            │    (Sonnet ↔ Haiku)     │
                                            │  + system prompt cache  │
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

- **OS:** Linux, macOS, or Windows with WSL2. Plain Windows is not supported — the setup steps use bash (`set -a && source .env`).
- **Python 3.10+** with `pip` (on some distros `pip` is a separate package, e.g. `python3-pip` on Fedora).
- **MySQL client CLI** for applying the schema: `brew install mysql-client` (macOS), `apt install mysql-client` (Debian/Ubuntu), `dnf install mysql` (Fedora).
- **TiDB Cloud cluster** — Starter for development, Essentials or Premium for production (TiCDC + Kafka sink require Essentials+).
- **Anthropic API key.** Workshop attendees will be given a shared key on the day; otherwise BYO.

---

## Python Dependencies

We strongly recommend a virtual environment so this project doesn't pollute your global Python install:

```bash
python3 -m venv .venv
source .venv/bin/activate    # on WSL: same; on Windows PowerShell: .venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

| Package | Required for |
|---|---|
| `pymysql` | TiDB connection (all components) |
| `python-dotenv` | `.env` loading (all components) |
| `anthropic` | Claude API — agent loop |
| `tiktoken` | Token counting for context budget |
| `sentence-transformers` | HuggingFace embedding model (used by both agent and embedding service) |
| `pytest` | Unit test suite |
| `kafka-python` | Kafka consumer — production TiCDC mode only (commented out by default) |
| `pyflink` | Flink windowing job — production only (commented out by default) |

---

## Quick Start

### 1. Clone and configure

```bash
git clone https://github.com/bernard-kavanagh/ev_charger_anomaly_detection.git
cd ev_charger_anomaly_detection
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your TiDB Cloud and Anthropic credentials
```

For `TIDB_SSL_CA`, point at your system CA bundle:
- macOS / Fedora: `/etc/ssl/cert.pem`
- Debian / Ubuntu: `/etc/ssl/certs/ca-certificates.crt`
- Or download `isrgrootx1.pem` from [letsencrypt.org/certs/isrgrootx1.pem](https://letsencrypt.org/certs/isrgrootx1.pem)

### 2. Apply schema

```bash
set -a && source .env && set +a

# All tables, indexes, and v2 features (FULLTEXT, anomaly_breakdown, supersession)
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

**Context Plane** — Three-Tier Agent Memory

| Table | Purpose |
|---|---|
| `session_state` | Active investigation state per session |
| `agent_reasoning` | **Episodic Memory**: Time-stamped investigation outcomes — confirmed, dismissed, escalated (with supersession tracking + FULLTEXT index) |
| `fleet_memory` | **Semantic Memory**: Fleet-wide learned knowledge, scoped (global/site/model), cosine-deduplicated at < 0.15 (+ FULLTEXT index) |
| `context_snapshots` | Cached, token-counted prompt fragments (operational infrastructure) |

> **Procedural Memory** — investigation strategies are currently encoded implicitly in semantic memory and the agent's system prompt. Explicit procedural memory with its own retrieval path is planned for the cognitive foundation project.

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

### Agent lifecycle and empirical results

For the full agent flow (trigger → context assembly → routing decision → loop → summary), the empirical validation across two clusters, and the reframed cost/quality thesis, see [AGENT_LIFECYCLE.md](AGENT_LIFECYCLE.md). It anchors every claim in this README to actual measured dispatch data.

### Context Assembly (Infrastructure-Level)

`assemble_context()` in `tool_handlers.py` builds the agent's system prompt under a 4,000-token hard cap (3,600 effective after 10% safety margin), loading sources in priority order:

| Priority | Source | Typical tokens | Notes |
|---|---|---|---|
| 1 | Charger profile (registry + snapshots) | ~80 | |
| 2 | Recent anomaly windows + breakdown | ~100–300 | |
| 3 | Active investigations (non-superseded) | ~100–200 | |
| 4 | Prior confirmed diagnoses (hybrid search) | ~200–500 | charger-scoped |
| 5 | Fleet knowledge (single-query scoped recall) | **capped at 500** | top-3 entries, content truncated to 80 chars (R1+R2) |

The R1+R2 caps on Tier 5 are critical: without them, the seed inflated proportionally as `fleet_memory` grew, with per-investigation cost climbing +54% across a 10-dispatch warm-up. With caps, the seed stays constant-size regardless of underlying memory volume.

Tier 5 also degrades gracefully: when `site_id` parsing fails (e.g. registry missing the charger), it falls back to `scope="any"` instead of skipping the tier entirely.

Context assembly runs **before** the model is invoked — zero LLM calls, pure SQL, ~50ms. The model never decides what to remember. The platform decides for it.

This eliminates the **Token Tax** in its narrow definition: the runtime cost of re-assembling context from scratch on every invocation. **Per-investigation API spend** is governed separately by the routing layer (below), not by context assembly.

### Routing Layer

After context assembly, code (not the model) inspects the top fleet memory matches and decides which model and how many tool rounds to use:

| Path | Trigger | Model | Tool rounds | Use case |
|---|---|---|---:|---|
| **SHORTCUT** | Any fleet match passes `confidence ≥ 0.85` AND `similarity ≥ 0.55` | Haiku | 3 | High-confidence pattern match — verify and checkpoint |
| **LOOKUP** | Trigger classifier flags status query | Haiku | 5 | Simple status/profile queries |
| **EXPLORE** | Default — no high-confidence match | Sonnet | 15 | Novel pattern discovery |

The SHORTCUT path drives the architecture's measurable cost reduction. It substitutes Haiku ($1/$5 per M) for Sonnet ($3/$15 per M) when the cognitive foundation has already recognized the pattern. The shortcut also skips the classify call entirely — saving ~50 tokens per investigation and eliminating one round-trip.

The routing layer scans all returned fleet matches (not just top-by-similarity) and picks the highest-confidence eligible entry. This is necessary because the most-similar entry isn't always the most-confident — a generic 0.72-confidence pattern with rich vocabulary overlap can outrank a charger-specific 0.97-confidence entry in cosine space.

See `tool_handlers.py:run_agent` for implementation, `AGENT_LIFECYCLE.md` for empirical validation.

### Custodial Duties (Memory Lifecycle)

1. **Write control:** Agent writes a reasoning checkpoint only when it reaches a conclusion (confirmed/dismissed/escalated). Intermediate reasoning stays ephemeral.
2. **Deduplication:** `write_fleet_memory()` checks cosine distance < 0.15 before insert. Near-duplicates are merged, incrementing `supporting_evidence_count`.
3. **Contradiction resolution:** Distance 0.15–0.40 in the same category and scope triggers auto-supersession via `superseded_by`. Prior diagnoses for the same charger are linked.
4. **Confidence decay:** `cleanup_job()` applies `confidence *= 0.95` monthly for memories older than 30 days. Memories below 0.30 are auto-deprecated.
5. **Compaction:** `compaction_job()` weekly re-clusters fleet memories, merging entries that have drifted within cosine distance 0.20.
6. **Forgetting:** TTL policies cap storage at ~960M rows. 90-day zero-access deprecation. Expired context snapshots pruned automatically.

The platform maintains memory through **five custodial duties**: write control (outcomes only), deduplication (cosine < 0.15), reconciliation (`superseded_by` chains), confidence decay (5% monthly, auto-deprecated below 0.30), and compaction (weekly re-clustering). These are SQL operations inside the cluster — not LLM calls, not external services.

> **Time-scale note:** The custodial duties operate at human time-scales (monthly decay, weekly compaction). They cap the *long-term equilibrium* size of `fleet_memory`, not per-investigation cost during a single dispatch loop. Per-investigation cost is governed by the R1+R2 caps on Tier 5 (read-side, immediate) and the routing layer (model selection). The duties keep the underlying store healthy; the routing layer makes investigations cheap.

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

- **Unified data substrate.** TiDB's native `VECTOR` type with HNSW indexing handles OLTP reads/writes and vector similarity search in one cluster. No frankenstack of bolt-on services — every additional system boundary adds sync lag, consistency gaps, and token debt.
- **Polling mode is first-class for Starter.** `--poll --once` is the intended embedding path when TiCDC changefeeds are unavailable.
- **Outcomes only in agent_reasoning.** Intermediate reasoning is not persisted. Only conclusions are written. Table bounded at O(investigations) not O(reasoning steps).
- **Semantic banding for embeddings.** `text_bander.py` converts raw metrics to natural language ("slightly elevated earth leakage", "error storm"). This is the single source of truth — both `embedding_service.py` and `stream_telemetry.py` import from the same module.
- **384-dim all-MiniLM-L6-v2.** Runs locally, no API key, no rate limits, no per-token cost.
- **Memory is infrastructure, not a feature.** Write control, deduplication, reconciliation, decay, compaction, and forgetting are all implemented as explicit lifecycle mechanisms — not bolted-on afterthoughts.
- **Routing is external to the LLM.** Sonnet doesn't self-shortcut on familiar patterns — it uses richer context to do *more* work, not less. The routing layer reads the cognitive foundation's top match and switches models in code. This is what makes the architecture's cost benefit measurable rather than hoped-for.
- **Bounded seed regardless of memory size.** Tier 5 is hard-capped at 500 tokens (R1) with content truncated to 80 chars (R2). The seed advertises that fleet patterns exist; the agent retrieves full content via `recall_fleet_memory` if needed. Without these caps, the seed inflates as memory grows and per-investigation cost climbs across runs.
- **Three measured dimensions for cost.** Tokens, dollars, and wall-clock latency don't move together. The dollar saving (~75% on production fleets) is the strongest claim because it's driven by the Sonnet → Haiku model swap, not just token volume.

---

## TiDB Cloud Tier Guide

| Feature | Starter | Essentials | Premium |
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
