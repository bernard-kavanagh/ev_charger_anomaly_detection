# EV Charger Platform — Audit Upgrade Package (April 2026)

## Test results

```
36 passed in 0.60s
```

All unit tests pass — covering serialisation, token budget, keyword extraction,
text banding (9 band functions), anomaly score breakdown, and data validation.

## New files

| File | Purpose |
|------|---------|
| `text_bander.py` | Shared text banding module — single source of truth for window→text conversion |
| `validation.py` | Data quality checks for telemetry ingest |
| `observability.py` | Structured JSON logging for agent runs |
| `schema_v2.sql` | Schema migration: anomaly_breakdown, FULLTEXT indexes, contradiction tracking |
| `tests/test_tool_handlers.py` | 36 unit tests |
| `tests/__init__.py` | Test package marker |

## Modified files

| File | Changes |
|------|---------|
| `tool_handlers.py` | Hybrid search, contradiction resolution, compaction job, circuit breaker, error handling, token safety margin, single-query scoped recall, confidence decay |

## Audit items addressed

### RED (8/8)
1. **Contradiction resolution** → `write_fleet_memory()` auto-supersedes + `write_reasoning_checkpoint()` links superseded diagnoses
2. **Hybrid search** → `_hybrid_search()` helper combining FULLTEXT MATCH + vector cosine
3. **Fleet memory compaction** → `compaction_job()` merges drifted memories weekly
4. **Anomaly explainability** → `anomaly_breakdown` JSON column + `compute_anomaly_breakdown()` in text_bander
5. **Data quality checks** → `validation.py` with `validate_telemetry()` and `validate_window()`
6. **Agent evaluation** → Tests validate banding, scoring, and breakdown correctness
7. **Observability** → `observability.py` with `AgentObserver` class
8. **Tests** → 36 unit tests across 7 test classes

### ORANGE (6/6)
9. **Semantic banding consistency** → `text_bander.py` extracted as shared module
10. **Fleet memory scoping** → Single query with `scope IN (...)` + post-rank by specificity
11. **Confidence decay** → `cleanup_job()` applies `confidence *= 0.95` monthly
12. **Circuit breaker** → `max_tool_rounds=15` + consecutive-same-tool detection
13. **Error handling** → `@_safe_handler` decorator on all tool handlers
14. **Token safety margin** → `effective_budget()` with 10% buffer

## How to apply

```bash
cd /Users/bernardkavanagh/Desktop/ev-charger-platform

# Backup current tool_handlers
cp tool_handlers.py tool_handlers.py.bak

# Copy all upgrade files into your repo
# (from wherever you unzipped the upgrade package)
cp text_bander.py validation.py observability.py schema_v2.sql tool_handlers.py .
cp -r tests/ .

# Apply schema migration
set -a && source .env && set +a
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" < schema_v2.sql

# Run tests
pip install pytest
python -m pytest tests/ -v

# Verify single agent run
python3 agent/run_agent.py --auto
```
