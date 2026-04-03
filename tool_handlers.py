"""
EV Charger IoT Agent — Tool Handler Backend (v2)
==================================================
Audit upgrade: April 2026

Changes from v1:
  - Hybrid search: vector cosine + FULLTEXT MATCH for fault code recall
  - Contradiction resolution: write_fleet_memory() auto-supersedes
  - Compaction job: weekly re-clustering + merge of drifted memories
  - Circuit breaker: max_tool_rounds + consecutive-same-tool detection
  - Error handling: all handlers wrapped in try/except → structured JSON
  - Token safety margin: 10% buffer (effective 3600 from 4000)
  - Single-query scoped recall: one SQL call with scope IN (...)
  - Confidence decay: cleanup_job() applies monthly decay
  - Observability: structured logging via AgentObserver
"""

import json
import os
import time
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pymysql
import tiktoken

from text_bander import TEXT_BUILDERS, build_window_text
from observability import AgentObserver

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_db():
    ssl_ca = os.environ.get("TIDB_SSL_CA")
    return pymysql.connect(
        host=os.environ["TIDB_HOST"],
        port=int(os.environ.get("TIDB_PORT", 4000)),
        user=os.environ["TIDB_USER"],
        password=os.environ["TIDB_PASSWORD"],
        database=os.environ["TIDB_DATABASE"],
        ssl={"ca": ssl_ca} if ssl_ca else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


# ---------------------------------------------------------------------------
# Embedding generation — HuggingFace all-MiniLM-L6-v2 (384-dim)
# ---------------------------------------------------------------------------

_EMBED_MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
_embed_model = None


def _get_embed_model():
    global _embed_model
    if _embed_model is None:
        from sentence_transformers import SentenceTransformer
        _embed_model = SentenceTransformer(_EMBED_MODEL_NAME)
    return _embed_model


def embed(text: str) -> list[float]:
    """Generate a 384-dim embedding using all-MiniLM-L6-v2."""
    return _get_embed_model().encode(text).tolist()


# ---------------------------------------------------------------------------
# Token counting with safety margin
# ---------------------------------------------------------------------------

try:
    _enc = tiktoken.encoding_for_model("gpt-4")
except Exception:
    _enc = None
    log.warning("tiktoken encoding not available, using word-count approximation")

# 10% safety margin: Claude's tokeniser can diverge from tiktoken by up
# to 15% on structured/technical text. At 4000 budget, that's 600 tokens
# of error — enough to silently truncate fleet memory.
TOKEN_BUDGET_DEFAULT = 4000
TOKEN_SAFETY_MARGIN = 0.10  # 10% buffer


def count_tokens(text: str) -> int:
    if _enc is not None:
        return len(_enc.encode(text))
    # Fallback: ~1.3 tokens per word is a reasonable approximation
    return int(len(text.split()) * 1.3)


def effective_budget(budget: int = TOKEN_BUDGET_DEFAULT) -> int:
    """Apply safety margin to token budget."""
    return int(budget * (1.0 - TOKEN_SAFETY_MARGIN))


# ---------------------------------------------------------------------------
# JSON serialisation helper
# ---------------------------------------------------------------------------

class TiDBEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def to_json(obj) -> str:
    return json.dumps(obj, cls=TiDBEncoder, indent=2)


# ---------------------------------------------------------------------------
# Error handling wrapper
# ---------------------------------------------------------------------------

def _safe_handler(func):
    """Wrap a tool handler so DB/embedding errors return structured JSON
    instead of crashing the agent loop."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except pymysql.Error as e:
            log.error(f"Database error in {func.__name__}: {e}")
            return to_json({
                "error": f"Database error: {e}",
                "tool": func.__name__,
                "retryable": True,
            })
        except Exception as e:
            log.error(f"Error in {func.__name__}: {e}", exc_info=True)
            return to_json({
                "error": f"Tool error: {e}",
                "tool": func.__name__,
                "retryable": False,
            })
    wrapper.__name__ = func.__name__
    return wrapper


# ============================================================================
# HYBRID SEARCH HELPER
# ============================================================================

def _hybrid_search(cur, table: str, vec_column: str,
                   text_columns: list[str], query_vec: str,
                   query_text: str, where_clauses: list[str],
                   params: list, limit: int = 5,
                   ft_index: Optional[str] = None) -> list[dict]:
    """
    Combined vector cosine + FULLTEXT search.

    Strategy:
    1. If query_text contains potential identifiers (error codes, model
       numbers), add a MATCH() AGAINST() boost.
    2. Always rank by vector distance as primary, but promote rows that
       also match keywords.
    3. Falls back gracefully if FULLTEXT index doesn't exist.
    """
    # Extract potential keywords (error codes, model names, etc.)
    keywords = _extract_keywords(query_text)

    if keywords and ft_index and text_columns:
        # Hybrid: vector + fulltext boost
        match_expr = f"MATCH({', '.join(text_columns)}) AGAINST(%s IN BOOLEAN MODE)"
        where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

        sql = f"""
            SELECT *, 
                   VEC_COSINE_DISTANCE({vec_column}, %s) AS distance,
                   ({match_expr}) AS ft_score
            FROM {table}
            {where_sql}
            ORDER BY (VEC_COSINE_DISTANCE({vec_column}, %s) - LEAST(0.1, ({match_expr}) * 0.05)) ASC
            LIMIT %s
        """
        keyword_str = " ".join(keywords)
        all_params = [query_vec, keyword_str] + params + [query_vec, keyword_str, limit]

        try:
            cur.execute(sql, all_params)
            return cur.fetchall()
        except pymysql.Error:
            # FULLTEXT index may not exist yet; fall back to vector-only
            log.debug(f"FULLTEXT search failed on {table}, falling back to vector-only")

    # Vector-only fallback
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    sql = f"""
        SELECT *,
               VEC_COSINE_DISTANCE({vec_column}, %s) AS distance
        FROM {table}
        {where_sql}
        ORDER BY distance ASC
        LIMIT %s
    """
    all_params = [query_vec] + params + [limit]
    cur.execute(sql, all_params)
    return cur.fetchall()


def _extract_keywords(text: str) -> list[str]:
    """Extract potential exact-match identifiers from query text.
    
    Catches error codes (E-001, GroundFailure), model names (Terra-54),
    firmware versions (3.1.2), and other structured identifiers that
    embedding models often fail to place close in vector space.
    """
    import re
    keywords = []

    # Error code patterns: E-001, F-002, GroundFailure, InternalError, etc.
    keywords.extend(re.findall(r'\b[A-Z]-\d{3}\b', text))
    keywords.extend(re.findall(r'\b(?:Ground|Internal|Power|Connector|High|Other)\w*(?:Failure|Error)\b', text))

    # Model names: Terra 54, RT50, Hypercharger 150, etc.
    keywords.extend(re.findall(r'\b(?:Terra|Troniq|Pulsar|Supernova|Hypercharger|PKM|RTM?)\s*\d+\b', text))

    # Firmware versions: 3.1.2, ABB 3.1.x
    keywords.extend(re.findall(r'\b\d+\.\d+\.[\dx]+\b', text))

    # Environment types
    keywords.extend(re.findall(r'\b(?:coastal|outdoor_exposed|outdoor_sheltered|indoor)\b', text))

    return list(set(keywords))


# ============================================================================
# TOOL HANDLERS
# ============================================================================

@_safe_handler
def search_similar_outages(window_id: int, limit: int = 5,
                           severity_filter: str = "any",
                           category_filter: str = "any") -> str:
    """Find outage catalog entries similar to a given anomaly window.
    Uses hybrid search: vector cosine + FULLTEXT keyword boost."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            "SELECT signature_vec, charger_id, window_start, anomaly_score, "
            "anomaly_breakdown "
            "FROM charger_windows WHERE id = %s", (window_id,)
        )
        window = cur.fetchone()
        if not window or not window["signature_vec"]:
            return to_json({"error": f"Window {window_id} not found or has no embedding."})

        where_clauses = []
        params = []
        if severity_filter != "any":
            where_clauses.append("severity = %s")
            params.append(severity_filter)
        if category_filter != "any":
            where_clauses.append("category = %s")
            params.append(category_filter)

        # Build a text query from the window's anomaly description
        query_text = build_window_text(window)

        results = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            ["pattern_name", "root_cause", "resolution"],
            window["signature_vec"], query_text,
            where_clauses, params, limit,
            ft_index="ft_outage_text",
        )

    return to_json({
        "window": {
            "id": window_id,
            "charger_id": window["charger_id"],
            "window_start": window["window_start"],
            "anomaly_score": window["anomaly_score"],
            "anomaly_breakdown": window.get("anomaly_breakdown"),
        },
        "similar_outages": [
            {**{k: v for k, v in r.items() if k not in ("signature_vec", "distance", "ft_score")},
             "similarity": round(1 - float(r["distance"]), 4)}
            for r in results
        ],
    })


@_safe_handler
def search_prior_diagnoses(observation_text: str,
                           charger_id: Optional[str] = None,
                           resolution_filter: str = "any",
                           limit: int = 5) -> str:
    """Find prior investigation outcomes similar to a given observation.
    Filters out superseded diagnoses."""
    vec = embed(observation_text)
    db = get_db()
    with db.cursor() as cur:
        where_clauses = [
            "ar.reasoning_vec IS NOT NULL",
            "ar.superseded_by IS NULL",  # Exclude superseded diagnoses
        ]
        params = [str(vec)]
        if charger_id:
            where_clauses.append("ar.charger_id = %s")
            params.append(charger_id)
        if resolution_filter != "any":
            where_clauses.append("ar.resolution = %s")
            params.append(resolution_filter)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT ar.id, ar.charger_id, ar.site_id, ar.session_id,
                   ar.created_at, ar.observation, ar.hypothesis,
                   ar.evidence_refs, ar.confidence, ar.resolution, ar.tags,
                   VEC_COSINE_DISTANCE(ar.reasoning_vec, %s) AS distance
            FROM agent_reasoning ar
            {where_sql}
            ORDER BY distance ASC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(sql, params)
        results = cur.fetchall()

    return to_json({
        "query": observation_text[:200],
        "diagnoses": [
            {**{k: v for k, v in r.items() if k != "distance"},
             "similarity": round(1 - float(r["distance"]), 4)}
            for r in results
        ],
    })


@_safe_handler
def write_reasoning_checkpoint(session_id: str, charger_id: str,
                                observation: str,
                                site_id: Optional[str] = None,
                                hypothesis: Optional[str] = None,
                                evidence_refs: Optional[list] = None,
                                confidence: float = 0.5,
                                resolution: str = "confirmed",
                                tags: Optional[list] = None) -> str:
    """Write a reasoning checkpoint to agent_reasoning.
    If this contradicts a prior diagnosis for the same charger, mark
    the prior one as superseded."""
    db = get_db()
    with db.cursor() as cur:
        # Insert the new reasoning
        cur.execute("""
            INSERT INTO agent_reasoning
                (charger_id, site_id, session_id, observation, hypothesis,
                 evidence_refs, confidence, resolution, tags)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            charger_id, site_id, session_id, observation, hypothesis,
            json.dumps(evidence_refs or []),
            confidence, resolution,
            json.dumps(tags or []),
        ))
        new_id = cur.lastrowid

        # Check for contradictions: if this is a confirmed diagnosis for a
        # charger that already has a different confirmed diagnosis, link them
        superseded_count = 0
        if resolution == "confirmed" and charger_id:
            cur.execute("""
                UPDATE agent_reasoning
                SET superseded_by = %s, superseded_at = NOW()
                WHERE charger_id = %s
                  AND resolution = 'confirmed'
                  AND id != %s
                  AND superseded_by IS NULL
                  AND created_at > NOW() - INTERVAL 30 DAY
            """, (new_id, charger_id, new_id))
            superseded_count = cur.rowcount

    return to_json({
        "status": "ok",
        "reasoning_id": new_id,
        "superseded_count": superseded_count,
        "message": f"Checkpoint saved for {charger_id}."
                   + (f" Superseded {superseded_count} prior diagnosis(es)."
                      if superseded_count > 0 else ""),
    })


@_safe_handler
def recall_fleet_memory(query_text: str,
                        scope: str = "any",
                        category_filter: str = "any",
                        limit: int = 5) -> str:
    """Retrieve relevant fleet knowledge using hybrid search.
    
    UPGRADE: Single query with scope IN (...) + post-ranking by specificity,
    instead of 3 sequential queries per scope level.
    """
    vec = embed(query_text)
    db = get_db()
    with db.cursor() as cur:
        where_clauses = ["fm.status = 'active'", "fm.memory_vec IS NOT NULL"]
        params = []

        if scope != "any":
            # Single query: match the specific scope OR global, then rank
            where_clauses.append("(fm.scope = %s OR fm.scope = 'global')")
            params.append(scope)
        if category_filter != "any":
            where_clauses.append("fm.category = %s")
            params.append(category_filter)

        # Hybrid search with FULLTEXT boost on content
        results = _hybrid_search(
            cur, "fleet_memory fm", "fm.memory_vec",
            ["fm.content"], str(vec), query_text,
            where_clauses, params, limit,
            ft_index="ft_memory_content",
        )

        # Post-rank: prefer specific scope over global at same distance
        if scope != "any":
            for r in results:
                if r.get("scope") == scope:
                    r["distance"] = float(r.get("distance", 1)) * 0.95  # 5% boost
            results.sort(key=lambda r: float(r.get("distance", 1)))

        # Update access counts
        if results:
            ids = [r["id"] for r in results]
            placeholders = ",".join(["%s"] * len(ids))
            cur.execute(f"""
                UPDATE fleet_memory
                SET access_count = access_count + 1, last_accessed = NOW()
                WHERE id IN ({placeholders})
            """, ids)

    return to_json({
        "query": query_text[:200],
        "memories": [
            {**{k: v for k, v in r.items()
                if k not in ("memory_vec", "distance", "ft_score")},
             "similarity": round(1 - float(r.get("distance", 1)), 4)}
            for r in results
        ],
    })


@_safe_handler
def write_fleet_memory(category: str, scope: str, content: str,
                       source_refs: Optional[list] = None,
                       confidence: float = 0.7) -> str:
    """Write or merge a fleet memory record.
    
    UPGRADE: Now checks for semantic contradictions within the same scope
    and auto-supersedes conflicting memories using the superseded_by column.
    """
    vec = embed(content)
    db = get_db()
    with db.cursor() as cur:
        # Check for near-duplicate (cosine distance < 0.15 = similarity > 0.85)
        cur.execute("""
            SELECT id, content, confidence, category,
                   VEC_COSINE_DISTANCE(memory_vec, %s) AS distance
            FROM fleet_memory
            WHERE status = 'active' AND scope = %s
            ORDER BY distance ASC
            LIMIT 3
        """, (str(vec), scope))
        nearby = cur.fetchall()

        if nearby:
            closest = nearby[0]
            dist = float(closest["distance"])

            if dist < 0.15:
                # Near-duplicate: merge (strengthen existing memory)
                cur.execute("""
                    UPDATE fleet_memory
                    SET content = %s,
                        confidence = GREATEST(confidence, %s),
                        supporting_evidence_count = supporting_evidence_count + 1,
                        source_refs = JSON_ARRAY_APPEND(
                            COALESCE(source_refs, JSON_ARRAY()), '$', %s
                        ),
                        memory_vec = %s
                    WHERE id = %s
                """, (content, confidence,
                      json.dumps(source_refs or []),
                      str(vec), closest["id"]))
                return to_json({
                    "status": "updated_existing",
                    "memory_id": closest["id"],
                    "message": "Near-duplicate found. Merged with existing memory.",
                })

            elif dist < 0.4 and closest["category"] == category:
                # Moderate similarity + same category = potential contradiction.
                # The new memory supersedes the old one.
                cur.execute("""
                    UPDATE fleet_memory
                    SET status = 'superseded', superseded_by = %s
                    WHERE id = %s
                """, (0, closest["id"]))  # Will update with real ID below

        # Insert new memory
        cur.execute("""
            INSERT INTO fleet_memory
                (category, scope, content, source_refs, confidence, memory_vec)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (category, scope, content,
              json.dumps(source_refs or []),
              confidence, str(vec)))
        memory_id = cur.lastrowid

        # Fix up the superseded_by pointer if we flagged a contradiction
        if nearby and float(nearby[0]["distance"]) < 0.4 and nearby[0]["category"] == category:
            cur.execute("""
                UPDATE fleet_memory
                SET superseded_by = %s
                WHERE id = %s AND status = 'superseded' AND superseded_by = 0
            """, (memory_id, nearby[0]["id"]))

    return to_json({
        "status": "created",
        "memory_id": memory_id,
        "message": f"Fleet memory recorded (scope: {scope}).",
    })


@_safe_handler
def get_charger_context(entity_id: str,
                        entity_type: str = "charger",
                        snapshot_types: Optional[list] = None) -> str:
    """Retrieve pre-assembled context snapshots for a charger or site."""
    if snapshot_types is None:
        snapshot_types = ["profile", "recent_anomalies"]

    db = get_db()
    with db.cursor() as cur:
        placeholders = ",".join(["%s"] * len(snapshot_types))
        cur.execute(f"""
            SELECT snapshot_type, content, token_count, created_at,
                   expires_at, is_stale
            FROM context_snapshots
            WHERE entity_type = %s AND entity_id = %s
              AND snapshot_type IN ({placeholders})
              AND expires_at > NOW()
            ORDER BY snapshot_type
        """, [entity_type, entity_id] + snapshot_types)
        snapshots = list(cur.fetchall())

    found_types = {s["snapshot_type"] for s in snapshots}
    missing = [t for t in snapshot_types if t not in found_types]

    for snap_type in missing:
        content = _build_snapshot(entity_id, entity_type, snap_type, db)
        if content:
            snapshots.append({
                "snapshot_type": snap_type,
                "content": content,
                "token_count": count_tokens(content),
                "created_at": datetime.now(),
                "is_stale": False,
            })

    total_tokens = sum(s["token_count"] for s in snapshots)

    return to_json({
        "entity_id": entity_id,
        "entity_type": entity_type,
        "snapshots": snapshots,
        "total_tokens": total_tokens,
    })


def _build_snapshot(entity_id: str, entity_type: str,
                    snap_type: str, db) -> Optional[str]:
    """Build a context snapshot on the fly and cache it."""
    try:
        with db.cursor() as cur:
            if entity_type == "charger" and snap_type == "profile":
                cur.execute("""
                    SELECT cr.*,
                           (SELECT COUNT(*) FROM charger_telemetry
                            WHERE charger_id = cr.charger_id
                            AND ts > NOW() - INTERVAL 24 HOUR) as msgs_24h
                    FROM charger_registry cr
                    WHERE cr.charger_id = %s
                """, (entity_id,))
                reg = cur.fetchone()
                if not reg:
                    return None
                content = (
                    f"Charger {reg['charger_id']} at site {reg['site_id']}. "
                    f"Model: {reg['manufacturer']} {reg['model']}, "
                    f"firmware {reg['firmware_version']}. "
                    f"Installed {reg['install_date']}, "
                    f"environment: {reg['environment']}. "
                    f"Max power: {reg['max_power_kw']}kW, "
                    f"{reg['connector_count']} connector(s). "
                    f"Total sessions: {reg['total_sessions']}, "
                    f"total energy: {reg['total_energy_kwh']}kWh. "
                    f"Last maintenance: {reg['last_maintenance']}. "
                    f"Messages in last 24h: {reg['msgs_24h']}."
                )

            elif entity_type == "charger" and snap_type == "recent_anomalies":
                cur.execute("""
                    SELECT window_start, anomaly_score, anomaly_flags,
                           anomaly_breakdown,
                           avg_power_w, voltage_stddev, max_temp_c, error_count
                    FROM charger_windows
                    WHERE charger_id = %s AND anomaly_score > 0
                    AND window_start > NOW() - INTERVAL 48 HOUR
                    ORDER BY window_start DESC
                    LIMIT 10
                """, (entity_id,))
                windows = cur.fetchall()
                if not windows:
                    content = f"No anomalies detected for {entity_id} in the last 48 hours."
                else:
                    lines = [f"{entity_id} recent anomalies (last 48h):"]
                    for w in windows:
                        breakdown = w.get("anomaly_breakdown") or {}
                        if isinstance(breakdown, str):
                            try:
                                breakdown = json.loads(breakdown)
                            except (json.JSONDecodeError, TypeError):
                                breakdown = {}
                        breakdown_str = ", ".join(
                            f"{k}={v}" for k, v in breakdown.items()
                        ) if breakdown else "no breakdown"
                        lines.append(
                            f"  {w['window_start']}: score={w['anomaly_score']}, "
                            f"breakdown=[{breakdown_str}], "
                            f"power={w['avg_power_w']}W, volt_std={w['voltage_stddev']}, "
                            f"temp={w['max_temp_c']}C, errors={w['error_count']}, "
                            f"flags={w['anomaly_flags']}"
                        )
                    content = "\n".join(lines)

            elif entity_type == "charger" and snap_type == "active_investigations":
                cur.execute("""
                    SELECT observation, hypothesis, confidence, resolution, created_at
                    FROM agent_reasoning
                    WHERE charger_id = %s
                      AND resolution IN ('confirmed', 'escalated')
                      AND superseded_by IS NULL
                    ORDER BY created_at DESC
                    LIMIT 5
                """, (entity_id,))
                investigations = cur.fetchall()
                if not investigations:
                    content = f"No active investigations for {entity_id}."
                else:
                    lines = [f"Recent investigations for {entity_id}:"]
                    for inv in investigations:
                        lines.append(
                            f"  [{inv['created_at']}] {inv['observation']} "
                            f"→ {inv['hypothesis'] or 'no hypothesis yet'} "
                            f"(confidence: {inv['confidence']}, {inv['resolution']})"
                        )
                    content = "\n".join(lines)
            else:
                return None

            # Cache the snapshot
            vec = embed(content)
            expires = datetime.now() + timedelta(minutes=30)
            cur.execute("""
                INSERT INTO context_snapshots
                    (entity_type, entity_id, snapshot_type, content,
                     token_count, expires_at, snapshot_vec)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    content = VALUES(content),
                    token_count = VALUES(token_count),
                    expires_at = VALUES(expires_at),
                    snapshot_vec = VALUES(snapshot_vec),
                    is_stale = FALSE
            """, (entity_type, entity_id, snap_type, content,
                  count_tokens(content), expires, str(vec)))

            return content

    except Exception as e:
        log.error(f"Failed to build snapshot {entity_type}/{entity_id}/{snap_type}: {e}")
        return None


@_safe_handler
def get_recent_windows(charger_id: str, hours_back: int = 6,
                       anomaly_only: bool = False) -> str:
    """Retrieve recent window aggregates including anomaly breakdown."""
    db = get_db()
    with db.cursor() as cur:
        anomaly_clause = "AND anomaly_score > 0" if anomaly_only else ""
        cur.execute(f"""
            SELECT id, window_start, window_end, msg_count,
                   avg_power_w, max_power_w, min_voltage_v, max_voltage_v,
                   voltage_stddev, avg_current_a, max_temp_c, avg_temp_c,
                   error_count, status_changes, distinct_errors,
                   avg_fan_rpm, max_earth_leak, anomaly_flags, anomaly_score,
                   anomaly_breakdown
            FROM charger_windows
            WHERE charger_id = %s
              AND window_start > NOW() - INTERVAL %s HOUR
              {anomaly_clause}
            ORDER BY window_start DESC
        """, (charger_id, hours_back))
        windows = cur.fetchall()

    return to_json({
        "charger_id": charger_id,
        "hours_back": hours_back,
        "window_count": len(windows),
        "windows": windows,
    })


@_safe_handler
def get_session_state(session_id: str,
                      user_id: Optional[str] = None) -> str:
    """Get or create session state."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("SELECT * FROM session_state WHERE session_id = %s",
                    (session_id,))
        session = cur.fetchone()

        if not session:
            cur.execute("""
                INSERT INTO session_state (session_id, user_id)
                VALUES (%s, %s)
            """, (session_id, user_id))
            session = {
                "session_id": session_id,
                "user_id": user_id,
                "focus_chargers": None,
                "focus_site": None,
                "investigation_summary": None,
                "token_budget": TOKEN_BUDGET_DEFAULT,
                "tokens_used": 0,
            }

    return to_json(session)


@_safe_handler
def update_session_state(session_id: str, **kwargs) -> str:
    """Update session state fields."""
    allowed = {"focus_chargers", "focus_site",
               "investigation_summary", "tokens_used"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}

    if not updates:
        return to_json({"status": "no_changes"})

    set_clauses = []
    params = []
    for k, v in updates.items():
        if k == "focus_chargers":
            set_clauses.append(f"{k} = %s")
            params.append(json.dumps(v))
        else:
            set_clauses.append(f"{k} = %s")
            params.append(v)

    params.append(session_id)

    db = get_db()
    with db.cursor() as cur:
        cur.execute(
            f"UPDATE session_state SET {', '.join(set_clauses)} "
            f"WHERE session_id = %s", params
        )

    return to_json({"status": "updated", "session_id": session_id})


# ============================================================================
# CONTEXT ASSEMBLY (with safety margin)
# ============================================================================

def assemble_context(session_id: str, charger_id: str,
                     trigger_text: str,
                     token_budget: int = TOKEN_BUDGET_DEFAULT) -> dict:
    """
    Assemble context for Claude's system prompt.
    
    UPGRADE: Uses effective_budget() with 10% safety margin.
    """
    t_start = time.monotonic()
    sections = []
    sources = []
    tokens_remaining = effective_budget(token_budget)

    # 1. Charger profile (~80 tokens)
    profile = json.loads(get_charger_context(
        charger_id, "charger", ["profile"]
    ))
    for snap in profile.get("snapshots", []):
        if snap["token_count"] <= tokens_remaining:
            sections.append(f"## Charger profile\n{snap['content']}")
            tokens_remaining -= snap["token_count"]
            sources.append("context_snapshot:profile")

    # 2. Recent anomalies (~100-300 tokens)
    anomalies = json.loads(get_charger_context(
        charger_id, "charger", ["recent_anomalies"]
    ))
    for snap in anomalies.get("snapshots", []):
        if snap["token_count"] <= tokens_remaining:
            sections.append(f"## Recent anomalies\n{snap['content']}")
            tokens_remaining -= snap["token_count"]
            sources.append("context_snapshot:recent_anomalies")

    # 3. Active investigations (~100-200 tokens)
    investigations = json.loads(get_charger_context(
        charger_id, "charger", ["active_investigations"]
    ))
    for snap in investigations.get("snapshots", []):
        if snap["token_count"] <= tokens_remaining:
            sections.append(f"## Recent investigations\n{snap['content']}")
            tokens_remaining -= snap["token_count"]
            sources.append("context_snapshot:active_investigations")

    # 4. Prior confirmed diagnoses (~200-500 tokens)
    if tokens_remaining > 200:
        similar = json.loads(search_prior_diagnoses(
            trigger_text, charger_id=charger_id,
            resolution_filter="confirmed", limit=3
        ))
        reasoning_lines = []
        for r in similar.get("diagnoses", []):
            line = (
                f"- [{r['created_at']}] Observed: {r['observation'][:120]}. "
                f"Hypothesis: {(r['hypothesis'] or 'none')[:120]}. "
                f"Resolution: {r['resolution']}, confidence: {r['confidence']}. "
                f"(similarity: {r['similarity']})"
            )
            line_tokens = count_tokens(line)
            if line_tokens <= tokens_remaining:
                reasoning_lines.append(line)
                tokens_remaining -= line_tokens
                sources.append(f"prior_diagnosis:{r['id']}")
        if reasoning_lines:
            sections.append(
                "## Prior confirmed diagnoses (similar cases)\n"
                + "\n".join(reasoning_lines)
            )

    # 5. Fleet memory — single query with scope ranking
    if tokens_remaining > 100:
        db = get_db()
        with db.cursor() as cur:
            cur.execute(
                "SELECT site_id, manufacturer, model, environment "
                "FROM charger_registry WHERE charger_id = %s",
                (charger_id,)
            )
            reg = cur.fetchone()

        if reg:
            # Try most specific scope; recall_fleet_memory now handles
            # IN (scope, 'global') in a single query
            primary_scope = f"site:{reg['site_id']}"
            memories = json.loads(recall_fleet_memory(
                trigger_text, scope=primary_scope, limit=5
            ))
            memory_lines = []
            for m in memories.get("memories", []):
                line = (
                    f"- [{m['category']}, {m['scope']}] "
                    f"{m['content'][:150]} "
                    f"(confidence: {m['confidence']}, "
                    f"evidence: {m['supporting_evidence_count']}x)"
                )
                line_tokens = count_tokens(line)
                if line_tokens <= tokens_remaining:
                    memory_lines.append(line)
                    tokens_remaining -= line_tokens
                    sources.append(f"fleet_memory:{m['id']}")
            if memory_lines:
                sections.append(
                    "## Fleet knowledge (relevant to this charger)\n"
                    + "\n".join(memory_lines)
                )

    system_context = "\n\n".join(sections)
    tokens_used = effective_budget(token_budget) - tokens_remaining
    assembly_ms = round((time.monotonic() - t_start) * 1000)

    return {
        "system_context": system_context,
        "tokens_used": tokens_used,
        "sources": sources,
        "assembly_ms": assembly_ms,
    }


# ============================================================================
# LIFECYCLE JOBS
# ============================================================================

def consolidation_job():
    """Promote confirmed reasoning patterns into fleet memory."""
    db = get_db()
    with db.cursor() as cur:
        cur.execute("""
            SELECT ar.charger_id, ar.site_id, ar.observation,
                   ar.hypothesis, ar.tags, ar.id,
                   cr.manufacturer, cr.model, cr.environment
            FROM agent_reasoning ar
            LEFT JOIN charger_registry cr ON ar.charger_id = cr.charger_id
            WHERE ar.resolution = 'confirmed'
              AND ar.superseded_by IS NULL
              AND ar.created_at > NOW() - INTERVAL 7 DAY
        """)
        confirmed = cur.fetchall()

        tag_groups = {}
        for r in confirmed:
            tags = json.loads(r["tags"]) if r["tags"] else []
            key = "|".join(sorted(tags)) if tags else "untagged"
            tag_groups.setdefault(key, []).append(r)

        promoted = 0
        for tag_key, group in tag_groups.items():
            if len(group) >= 3:
                charger_ids = [g["charger_id"] for g in group]
                sample = group[0]
                content = (
                    f"Confirmed pattern across {len(group)} chargers "
                    f"({', '.join(charger_ids[:5])}): "
                    f"{sample['hypothesis'] or sample['observation']}. "
                    f"Tags: {tag_key}."
                )
                environments = {g["environment"] for g in group if g.get("environment")}
                models = {f"{g['manufacturer']}-{g['model']}" for g in group
                          if g.get("manufacturer")}
                if len(models) == 1:
                    scope = f"model:{models.pop()}"
                elif len(environments) == 1:
                    scope = f"environment:{environments.pop()}"
                else:
                    scope = "global"

                write_fleet_memory(
                    category="pattern", scope=scope, content=content,
                    source_refs=[f"agent_reasoning:{g['id']}" for g in group],
                    confidence=0.8,
                )
                ids = [g["id"] for g in group]
                placeholders = ",".join(["%s"] * len(ids))
                cur.execute(f"""
                    UPDATE agent_reasoning
                    SET resolution = 'promoted'
                    WHERE id IN ({placeholders})
                """, ids)
                promoted += 1

    return f"Consolidation complete. Promoted {promoted} pattern(s) to fleet memory."


def cleanup_job():
    """
    Periodic cleanup with confidence decay.
    
    UPGRADE: Applies monthly confidence decay (0.95x) to memories older
    than 30 days, regardless of access frequency. Memories that decay
    below 0.3 confidence are deprecated.
    """
    db = get_db()
    with db.cursor() as cur:
        # Expire inactive sessions
        cur.execute("""
            DELETE FROM session_state
            WHERE last_active < NOW() - INTERVAL 24 HOUR
        """)
        sessions_cleaned = cur.rowcount

        # Remove expired context snapshots
        cur.execute("""
            DELETE FROM context_snapshots
            WHERE expires_at < NOW()
        """)
        snapshots_cleaned = cur.rowcount

        # Deprecate memories with 0 access in 90 days
        cur.execute("""
            UPDATE fleet_memory
            SET status = 'deprecated'
            WHERE status = 'active'
              AND access_count = 0
              AND created_at < NOW() - INTERVAL 90 DAY
        """)
        memories_deprecated = cur.rowcount

        # Confidence decay: 5% reduction per month for memories > 30 days old
        cur.execute("""
            UPDATE fleet_memory
            SET confidence = ROUND(confidence * 0.95, 2)
            WHERE status = 'active'
              AND updated_at < NOW() - INTERVAL 30 DAY
        """)
        memories_decayed = cur.rowcount

        # Auto-deprecate memories that have decayed below threshold
        cur.execute("""
            UPDATE fleet_memory
            SET status = 'deprecated'
            WHERE status = 'active'
              AND confidence < 0.30
        """)
        memories_auto_deprecated = cur.rowcount

    return (
        f"Cleanup: {sessions_cleaned} sessions expired, "
        f"{snapshots_cleaned} snapshots pruned, "
        f"{memories_deprecated} memories deprecated (no access), "
        f"{memories_decayed} memories confidence-decayed, "
        f"{memories_auto_deprecated} memories auto-deprecated (below 0.30)."
    )


def compaction_job():
    """
    Weekly fleet memory compaction.
    
    NEW: Re-clusters active memories by scope, merges entries that have
    drifted close together over time (< 0.20 cosine distance), and
    consolidates supporting evidence counts.
    """
    db = get_db()
    merged = 0
    with db.cursor() as cur:
        # Get all active scopes
        cur.execute("""
            SELECT DISTINCT scope FROM fleet_memory WHERE status = 'active'
        """)
        scopes = [r["scope"] for r in cur.fetchall()]

        for scope in scopes:
            # Get all active memories in this scope
            cur.execute("""
                SELECT id, content, confidence, supporting_evidence_count,
                       memory_vec, category
                FROM fleet_memory
                WHERE status = 'active' AND scope = %s
                ORDER BY confidence DESC
            """, (scope,))
            memories = cur.fetchall()

            if len(memories) < 2:
                continue

            # For each pair, check if they've drifted close enough to merge
            merged_ids = set()
            for i, m1 in enumerate(memories):
                if m1["id"] in merged_ids:
                    continue
                if not m1.get("memory_vec"):
                    continue

                for m2 in memories[i + 1:]:
                    if m2["id"] in merged_ids:
                        continue
                    if not m2.get("memory_vec"):
                        continue
                    if m1["category"] != m2["category"]:
                        continue

                    # Check cosine distance
                    cur.execute("""
                        SELECT VEC_COSINE_DISTANCE(%s, %s) AS dist
                    """, (m1["memory_vec"], m2["memory_vec"]))
                    dist = float(cur.fetchone()["dist"])

                    if dist < 0.20:
                        # Merge: keep the higher-confidence one, absorb the other
                        keep = m1 if float(m1["confidence"]) >= float(m2["confidence"]) else m2
                        absorb = m2 if keep == m1 else m1

                        new_evidence = (
                            int(keep["supporting_evidence_count"])
                            + int(absorb["supporting_evidence_count"])
                        )
                        new_confidence = min(
                            1.0,
                            max(float(keep["confidence"]), float(absorb["confidence"]))
                        )

                        cur.execute("""
                            UPDATE fleet_memory
                            SET supporting_evidence_count = %s,
                                confidence = %s
                            WHERE id = %s
                        """, (new_evidence, new_confidence, keep["id"]))

                        cur.execute("""
                            UPDATE fleet_memory
                            SET status = 'superseded', superseded_by = %s
                            WHERE id = %s
                        """, (keep["id"], absorb["id"]))

                        merged_ids.add(absorb["id"])
                        merged += 1

    return f"Compaction complete. Merged {merged} memory pair(s)."


def refresh_snapshots_job(charger_ids: Optional[list] = None):
    """Refresh context snapshots for chargers with recent activity."""
    db = get_db()
    with db.cursor() as cur:
        if charger_ids:
            placeholders = ",".join(["%s"] * len(charger_ids))
            cur.execute(f"""
                SELECT DISTINCT charger_id FROM charger_windows
                WHERE charger_id IN ({placeholders})
                AND window_start > NOW() - INTERVAL 30 MINUTE
            """, charger_ids)
        else:
            cur.execute("""
                SELECT DISTINCT charger_id FROM charger_windows
                WHERE anomaly_score > 0.3
                AND window_start > NOW() - INTERVAL 30 MINUTE
            """)
        active_chargers = [r["charger_id"] for r in cur.fetchall()]

    refreshed = 0
    for cid in active_chargers:
        for snap_type in ["profile", "recent_anomalies", "active_investigations"]:
            _build_snapshot(cid, "charger", snap_type, db)
            refreshed += 1

    return f"Refreshed {refreshed} snapshots for {len(active_chargers)} chargers."


# ============================================================================
# TOOL DISPATCHER
# ============================================================================

TOOL_HANDLERS = {
    "search_similar_outages": search_similar_outages,
    "search_prior_diagnoses": search_prior_diagnoses,
    "write_reasoning_checkpoint": write_reasoning_checkpoint,
    "recall_fleet_memory": recall_fleet_memory,
    "write_fleet_memory": write_fleet_memory,
    "get_charger_context": get_charger_context,
    "get_recent_windows": get_recent_windows,
    "get_session_state": get_session_state,
    "update_session_state": update_session_state,
}


def handle_tool_call(tool_name: str, tool_input: dict) -> str:
    """Dispatch a Claude tool call to the appropriate handler."""
    handler = TOOL_HANDLERS.get(tool_name)
    if not handler:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})
    return handler(**tool_input)


# ============================================================================
# AGENT LOOP (with circuit breaker + observability)
# ============================================================================

def run_agent(session_id: str, user_id: str, charger_id: str,
              trigger: str,
              max_tool_rounds: int = 15) -> dict:
    """
    Complete agent invocation with circuit breaker and observability.
    
    UPGRADE:
    - max_tool_rounds: hard cap on agent loop iterations (default 15)
    - Circuit breaker: halts if same tool called 3x consecutively
    - Structured logging via AgentObserver
    """
    import anthropic

    obs = AgentObserver(session_id=session_id, charger_id=charger_id)

    # Step 1: Assemble context
    ctx = assemble_context(session_id, charger_id, trigger)
    obs.context_assembled(
        tokens_used=ctx["tokens_used"],
        token_budget=TOKEN_BUDGET_DEFAULT,
        sources=ctx["sources"],
        assembly_ms=ctx.get("assembly_ms", 0),
    )

    # Step 2: Load tool definitions
    _tools_path = Path(__file__).parent / "tool_definitions.json"
    with open(_tools_path) as f:
        tools_config = json.load(f)

    # Step 3: Build system prompt
    system_prompt = f"""You are an EV charger fleet diagnostic agent. You analyse
IoT telemetry data from 20,000 electric vehicle charge points to detect outages,
service degradation, and failure patterns.

You have access to tools for searching similar outages, retrieving and writing
reasoning checkpoints, and recalling fleet-wide knowledge. Always checkpoint
your reasoning after reaching a hypothesis. Promote confirmed patterns to
fleet memory.

When reporting anomaly scores, always include the breakdown (which features
contributed to the score) so operators can understand what triggered the alert.

Current session: {session_id}
Current charger: {charger_id}

{ctx['system_context']}

Context sources: {', '.join(ctx['sources'])}
Tokens used for context: {ctx['tokens_used']}/{TOKEN_BUDGET_DEFAULT}
"""

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": trigger}]

    tool_call_counts: dict[str, int] = {}
    last_tools: list[str] = []  # Track consecutive tool calls

    # Step 4: Agent loop with circuit breaker
    for iteration in range(1, max_tool_rounds + 1):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system_prompt,
            tools=tools_config["tools"],
            messages=messages,
        )

        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        tool_calls = [b for b in assistant_content if b.type == "tool_use"]
        if not tool_calls:
            break

        # Circuit breaker: same tool 3x consecutively
        current_tools = [tc.name for tc in tool_calls]
        obs.loop_iteration(iteration, current_tools)

        if len(last_tools) >= 2 and all(t == current_tools[0] for t in last_tools[-2:] + current_tools[:1]):
            obs.circuit_breaker("same_tool_3x_consecutive", current_tools[0])
            log.warning(
                f"Circuit breaker: {current_tools[0]} called 3x consecutively. "
                f"Halting agent loop at iteration {iteration}."
            )
            break

        last_tools.extend(current_tools)

        # Handle each tool call with observability
        tool_results = []
        for tc in tool_calls:
            obs.tool_call_start(tc.name, tc.input)
            tool_call_counts[tc.name] = tool_call_counts.get(tc.name, 0) + 1

            result = handle_tool_call(tc.name, tc.input)

            # Extract result size and top distance for observability
            try:
                parsed = json.loads(result)
                result_size = len(parsed.get("similar_outages", []) or
                                  parsed.get("diagnoses", []) or
                                  parsed.get("memories", []) or
                                  parsed.get("windows", []) or [])
            except (json.JSONDecodeError, AttributeError):
                result_size = 0

            obs.tool_call_end(tc.name, result_size=result_size)

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tc.id,
                "content": result,
            })
        messages.append({"role": "user", "content": tool_results})

    # Extract final text
    final_text = "\n".join(
        b.text for b in assistant_content if b.type == "text"
    )

    # Update session
    update_session_state(
        session_id,
        focus_chargers=[charger_id],
        investigation_summary=final_text[:500],
        tokens_used=ctx["tokens_used"],
    )

    obs.agent_complete()

    return {
        "response": final_text,
        "tool_calls": tool_call_counts,
        "context_sources": ctx["sources"],
        "context_tokens": ctx["tokens_used"],
        "observability": obs.summary(),
    }
