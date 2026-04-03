# EV Charger Platform — Post-Upgrade Integrity Check

Run this with: `claude "$(cat check-upgrade.md)"`

## Task

You are auditing the ev-charger-platform repo after a v2 upgrade. Check that all files, imports, dependencies, and schema are consistent. Report any issues.

## Checks to perform

### 1. File inventory
Verify these files exist in the repo root:
- `tool_handlers.py` (upgraded v2 — should import from `text_bander`, `observability`)
- `text_bander.py` (new — shared banding module)
- `validation.py` (new — data quality checks)
- `observability.py` (new — AgentObserver class)
- `schema.sql` (original base schema)
- `schema_v2.sql` (new — FULLTEXT indexes, anomaly_breakdown, supersession)
- `tool_definitions.json` (unchanged — 9 tools)
- `README.md` (updated to reflect v2)
- `UPGRADE.md` (new — changelog)
- `requirements.txt`
- `tests/test_tool_handlers.py` (new — 36 tests)
- `tests/__init__.py`
- `agent/run_agent.py`
- `agent/dispatch.py`
- `agent/__init__.py`
- `embedding/embedding_service.py`
- `seed/stream_telemetry.py`
- `seed/seed_charger_registry.py`
- `seed/seed_outage_catalog.py`
- `flink/flink_windowing_job.py`
- `config/ticdc_config.toml`
- `reset.sql`

Run: `find . -name '*.py' -o -name '*.sql' -o -name '*.json' -o -name '*.toml' -o -name '*.md' -o -name '*.txt' | grep -v __pycache__ | grep -v .git | sort`

### 2. Import consistency
Check that `tool_handlers.py` successfully imports from these local modules:
```bash
python3 -c "from text_bander import TEXT_BUILDERS, build_window_text, compute_anomaly_breakdown, compute_anomaly_score; print('text_bander OK')"
python3 -c "from validation import validate_telemetry, validate_window; print('validation OK')"
python3 -c "from observability import AgentObserver; print('observability OK')"
python3 -c "from tool_handlers import run_agent, assemble_context, handle_tool_call, compaction_job, cleanup_job, consolidation_job, effective_budget, _extract_keywords, _hybrid_search; print('tool_handlers OK')"
```

### 3. Test suite passes
```bash
python3 -m pytest tests/ -v --tb=short
```
Expected: 36 passed, 0 failed, 0 errors.

### 4. Schema consistency
Check that schema_v2.sql has been applied to the live cluster:
```bash
set -a && source .env && set +a
# Check anomaly_breakdown column exists
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" \
  -e "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name='charger_windows' AND column_name='anomaly_breakdown';"

# Check FULLTEXT indexes exist
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" \
  -e "SELECT TABLE_NAME, INDEX_NAME FROM information_schema.statistics WHERE INDEX_TYPE='FULLTEXT' AND TABLE_SCHEMA=DATABASE();"

# Check superseded_by column exists on agent_reasoning
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" \
  -e "SELECT COLUMN_NAME FROM information_schema.columns WHERE table_name='agent_reasoning' AND column_name='superseded_by';"
```

### 5. Key function signatures
Verify v2-specific functions exist in tool_handlers.py:
```bash
grep -n "def compaction_job" tool_handlers.py
grep -n "def _hybrid_search" tool_handlers.py
grep -n "def _extract_keywords" tool_handlers.py
grep -n "def effective_budget" tool_handlers.py
grep -n "@_safe_handler" tool_handlers.py | head -5
grep -n "circuit_breaker" tool_handlers.py | head -3
grep -n "superseded_by" tool_handlers.py | head -3
grep -n "max_tool_rounds" tool_handlers.py
```

### 6. Dependencies check
```bash
python3 -c "import pymysql; print(f'pymysql {pymysql.__version__}')"
python3 -c "import anthropic; print(f'anthropic {anthropic.__version__}')"
python3 -c "import tiktoken; print('tiktoken OK')"
python3 -c "import sentence_transformers; print(f'sentence-transformers {sentence_transformers.__version__}')"
python3 -c "import pytest; print(f'pytest {pytest.__version__}')"
```

### 7. Agent dry run (requires DB)
```bash
set -a && source .env && set +a
# Check if there's data to investigate
mysql -h "$TIDB_HOST" -P "$TIDB_PORT" -u "$TIDB_USER" -p"$TIDB_PASSWORD" \
  --ssl-ca="$TIDB_SSL_CA" "$TIDB_DATABASE" \
  -e "SELECT COUNT(*) as total_windows, SUM(anomaly_score > 0) as anomalous, MAX(anomaly_score) as max_score FROM charger_windows WHERE window_start > NOW() - INTERVAL 24 HOUR;"
```

### 8. Embedding service uses shared text_bander
Check that embedding_service.py imports from text_bander (or if it still has its own inline builders, flag this as a sync risk):
```bash
grep -n "from text_bander import\|import text_bander" embedding/embedding_service.py || echo "WARNING: embedding_service.py does NOT import from text_bander — banding vocabulary may drift"
grep -n "def build_window_text" embedding/embedding_service.py && echo "WARNING: embedding_service.py has its own build_window_text — should use text_bander.py instead"
```

## Report format

For each check, report:
- PASS (green) if everything is correct
- WARN (orange) if functional but needs attention
- FAIL (red) if broken or missing

Summarise at the end with a count: X PASS, Y WARN, Z FAIL.

If check 8 shows a WARNING, explain that `embedding_service.py` should be updated to import `TEXT_BUILDERS` from `text_bander.py` instead of defining its own text builder functions, to prevent banding vocabulary drift between the embedding pipeline and the agent's context assembly.
