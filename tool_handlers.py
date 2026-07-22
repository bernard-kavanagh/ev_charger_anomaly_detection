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
import threading
import time
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pymysql
import tiktoken
from dbutils.pooled_db import PooledDB

from text_bander import TEXT_BUILDERS, build_window_text
from observability import AgentObserver

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    # Double-checked locking: PooledDB singleton init is not safe to run
    # concurrently from multiple threads (creator/connection state is
    # mutated in __init__). The lock serializes the first call.
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                ssl_ca = os.environ.get("TIDB_SSL_CA")
                _pool = PooledDB(
                    creator=pymysql,
                    maxconnections=20,
                    mincached=2,
                    maxcached=5,
                    blocking=True,
                    host=os.environ["TIDB_HOST"],
                    port=int(os.environ.get("TIDB_PORT", 4000)),
                    user=os.environ["TIDB_USER"],
                    password=os.environ["TIDB_PASSWORD"],
                    database=os.environ["TIDB_DATABASE"],
                    ssl={"ca": ssl_ca} if ssl_ca else None,
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True,
                )
    return _pool


@contextmanager
def get_db():
    conn = _get_pool().connection()
    try:
        yield conn
    finally:
        conn.close()  # returns to pool, does not actually close


# ---------------------------------------------------------------------------
# Embedding generation — HuggingFace all-MiniLM-L6-v2 (384-dim)
# ---------------------------------------------------------------------------

_EMBED_MODEL_NAME = os.environ.get("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
_embed_model = None
_embed_model_lock = threading.Lock()


def _get_embed_model():
    # Double-checked locking: avoid taking the lock on the hot path once
    # the singleton is initialized. The torch meta-tensor copy in
    # SentenceTransformer.__init__ is not safe to run concurrently from
    # multiple threads, so the first call must be serialized.
    global _embed_model
    if _embed_model is None:
        with _embed_model_lock:
            if _embed_model is None:
                from sentence_transformers import SentenceTransformer
                _embed_model = SentenceTransformer(_EMBED_MODEL_NAME)
    return _embed_model


def warmup_embed_model() -> None:
    """Load the embedding singleton on the calling thread.

    Call this once from the main thread before spawning workers so the
    racy first-init happens in a single-threaded context.
    """
    _get_embed_model()


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

# R1: hard cap on the Tier 5 (fleet_memory) section of assemble_context.
# Tier 5 used to consume whatever remained of the global budget after
# Tiers 1-4 — which let the seed inflate as fleet_memory grew. R2
# (limit=3, 80-char truncation) reduced typical Tier 5 to ~120 tokens.
# This cap is defense-in-depth: even if those parameters regress later,
# the seed's fleet_memory section can never exceed TIER5_MAX_TOKENS.
TIER5_MAX_TOKENS = 500


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
        except TypeError as e:
            # Schema mismatch from the model (missing/extra args). The
            # agent can correct itself on the next turn — surface a
            # retryable error rather than a crash-shaped one.
            log.warning(
                f"Invalid arguments for {func.__name__}: {e} "
                f"(args={args}, kwargs={list(kwargs.keys())})"
            )
            return to_json({
                "error": f"Invalid arguments for {func.__name__}: {e}",
                "tool": func.__name__,
                "retryable": True,
                "provided_args": list(kwargs.keys()),
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

# No-injection invariant: every table / vector-column / fulltext-column
# identifier that reaches an f-string below MUST be a member of this
# frozenset. These are the exact identifier strings (including query
# aliases like "fleet_memory fm" / "fm.content") that the call sites pass.
# Anything else raises ValueError before interpolation, so the f-strings
# can never carry attacker- or bug-supplied SQL. Keep this in sync with
# the FULLTEXT / VECTOR INDEX definitions in schema.sql.
_ALLOWED_SQL_IDENTIFIERS = frozenset({
    # tables (may include a query alias)
    "outage_catalog",
    "fleet_memory fm",
    "agent_reasoning",
    # vector columns
    "signature_vec",
    "fm.memory_vec",
    "reasoning_vec",
    # fulltext columns — one per FULLTEXT INDEX in schema.sql
    "root_cause",       # ft_outage_root_cause
    "resolution",       # ft_outage_resolution
    "fm.content",       # ft_memory_content (fleet_memory aliased fm)
    "content",          # ft_memory_content (unaliased)
    "observation",      # ft_reasoning_obs
})


def _validate_identifiers(table: str, vec_column: str,
                          ft_columns: list[str]) -> None:
    """Reject any identifier not on the schema allow-list.

    Guards the f-string interpolation in the SQL builders: table,
    vec_column, and every fulltext column must be known-safe.
    """
    for ident in [table, vec_column, *ft_columns]:
        if ident not in _ALLOWED_SQL_IDENTIFIERS:
            raise ValueError(f"Disallowed SQL identifier: {ident!r}")


def _build_ft_expr(ft_columns: list[str]) -> str:
    """Build the TiDB full-text score expression.

    TiDB FULLTEXT indexes are one column per index, so per-column
    FTS_MATCH_WORD() calls are combined with GREATEST(). Each call
    carries one %s placeholder for the keyword string.
    """
    if len(ft_columns) == 1:
        return f"FTS_MATCH_WORD(%s, {ft_columns[0]})"
    inner = ", ".join(f"FTS_MATCH_WORD(%s, {col})" for col in ft_columns)
    return f"GREATEST({inner})"


def _build_hybrid_sql(table: str, vec_column: str, ft_columns: list[str],
                      where_clauses: list[str]) -> str:
    """Build the hybrid vector + full-text SQL.

    Vector distance stays the primary rank; a matching keyword shaves up
    to 0.1 off the distance (ft_score * 0.05, clamped by LEAST). The
    GREATEST/FTS_MATCH_WORD expression is INLINED into ORDER BY rather
    than referencing the ft_score alias — inlining always parses on
    TiDB, sidestepping any alias-in-ORDER-BY restriction.
    """
    _validate_identifiers(table, vec_column, ft_columns)
    ft_expr = _build_ft_expr(ft_columns)
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return f"""
        SELECT *,
               VEC_COSINE_DISTANCE({vec_column}, %s) AS distance,
               ({ft_expr}) AS ft_score
        FROM {table}
        {where_sql}
        ORDER BY (VEC_COSINE_DISTANCE({vec_column}, %s) - LEAST(0.1, ({ft_expr}) * 0.05)) ASC
        LIMIT %s
    """


def _build_vector_sql(table: str, vec_column: str,
                      where_clauses: list[str]) -> str:
    """Build the vector-only fallback SQL."""
    _validate_identifiers(table, vec_column, [])
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    return f"""
        SELECT *,
               VEC_COSINE_DISTANCE({vec_column}, %s) AS distance
        FROM {table}
        {where_sql}
        ORDER BY distance ASC
        LIMIT %s
    """


def _hybrid_search(cur, table: str, vec_column: str,
                   ft_columns: Optional[list[str]], query_vec: str,
                   query_text: str, where_clauses: list[str],
                   params: list, limit: int = 5) -> tuple[list[dict], bool]:
    """
    Combined vector cosine + TiDB full-text search.

    Strategy:
    1. If query_text yields keywords (error codes, model numbers, etc.)
       and ft_columns are given, add an FTS_MATCH_WORD() boost — vector
       distance stays the primary rank, keyword matches promote rows.
    2. Falls back to vector-only if the full-text query errors.

    Returns (rows, ft_used) where ft_used is True only when the hybrid
    full-text query actually ran. When it is False despite keywords
    being present, the caller surfaces a fallback warning sentinel.
    """
    # Extract potential keywords (error codes, model names, etc.)
    keywords = _extract_keywords(query_text)

    if keywords and ft_columns:
        sql = _build_hybrid_sql(table, vec_column, ft_columns, where_clauses)
        keyword_str = " ".join(keywords)
        # Placeholder order: SELECT distance vec, one keyword per ft column
        # (SELECT ft_score), WHERE params, ORDER BY distance vec, one
        # keyword per ft column (ORDER BY ft_expr), LIMIT.
        n = len(ft_columns)
        all_params = (
            [query_vec] + [keyword_str] * n + params
            + [query_vec] + [keyword_str] * n + [limit]
        )
        try:
            cur.execute(sql, all_params)
            return cur.fetchall(), True
        except pymysql.Error as e:
            # Full-text query failed (e.g. missing index). Fall back to
            # vector-only, but at WARNING with a grep-able sentinel — a
            # silent debug-level fallback previously masked the broken
            # MySQL-only full-text syntax on TiDB.
            log.warning(
                "FULLTEXT_FALLBACK: hybrid full-text search failed on "
                "table %s: %s; falling back to vector-only",
                table, e,
            )

    # Vector-only fallback
    sql = _build_vector_sql(table, vec_column, where_clauses)
    all_params = [query_vec] + params + [limit]
    cur.execute(sql, all_params)
    return cur.fetchall(), False


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
# DERIVED CONFIDENCE
# ============================================================================
#
# Decision-grade confidence is computed HERE, from observable events, and is
# never taken from the model's self-report. The model's self-assessment is
# stored in fleet_memory.model_confidence / agent_reasoning.model_confidence
# as telemetry only. See migrations/schema_v3_derived_confidence.sql.

# (alpha, beta) Beta priors per provenance — the base rate before evidence.
_CONFIDENCE_PRIORS = {
    "session": (1, 1),       # base 0.50 — unverified model self-report
    "consolidated": (3, 1),  # base 0.75 — corroborated across >=3 cases
    "verified": (6, 1),      # base ~0.86 — field-verified outcome
}
_CONFIDENCE_FLOOR = 0.05
_CONFIDENCE_CEIL = 0.99


def derive_confidence(provenance: str, confirmations: int,
                      contradictions: int) -> float:
    """Derive decision-grade confidence from observed outcomes.

    Beta-posterior mean with provenance-dependent priors:

        (confirmations + alpha) / (confirmations + contradictions + alpha + beta)

    Priors (alpha, beta) encode the base rate before any evidence:
        session      -> (1, 1)  base 0.50  (unverified model self-report)
        consolidated -> (3, 1)  base 0.75  (corroborated across >=3 cases)
        verified     -> (6, 1)  base ~0.86 (field-verified outcome)

    Properties:
      * Monotonically INCREASING in ``confirmations``.
      * Monotonically DECREASING in ``contradictions``.
      * DIMINISHING RETURNS: each additional confirmation moves the
        posterior less than the previous one (Δ from 3→4 < Δ from 0→1).
      * BOUNDED below 1.0: clamped to [0.05, 0.99] after the posterior is
        computed, so no amount of corroboration ever yields certainty.

    Pure function: no DB access, no model self-report as input. Unknown
    provenance falls back to the ``session`` prior.
    """
    alpha, beta = _CONFIDENCE_PRIORS.get(provenance, _CONFIDENCE_PRIORS["session"])
    confirmations = max(0, int(confirmations))
    contradictions = max(0, int(contradictions))
    posterior = (confirmations + alpha) / (
        confirmations + contradictions + alpha + beta
    )
    # Clamp BEFORE rounding so a posterior like 0.998 becomes 0.99, never 1.0.
    clamped = min(_CONFIDENCE_CEIL, max(_CONFIDENCE_FLOOR, posterior))
    return round(clamped, 2)


# ============================================================================
# TOOL HANDLERS
# ============================================================================

@_safe_handler
def search_similar_outages(window_id: int, limit: int = 5,
                           severity_filter: str = "any",
                           category_filter: str = "any") -> str:
    """Find outage catalog entries similar to a given anomaly window.
    Uses hybrid search: vector cosine + FULLTEXT keyword boost."""
    with get_db() as db:
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

            # Only root_cause and resolution have FULLTEXT indexes in
            # schema.sql (ft_outage_root_cause, ft_outage_resolution);
            # pattern_name does not, so it is excluded from ft_columns.
            results, ft_used = _hybrid_search(
                cur, "outage_catalog", "signature_vec",
                ["root_cause", "resolution"],
                window["signature_vec"], query_text,
                where_clauses, params, limit,
            )

    # Surface a grep-able sentinel (mirrors the Tier 4/5 pattern in
    # assemble_context) when keywords were present but the full-text
    # query fell back to vector-only.
    warnings = []
    if not ft_used and _extract_keywords(query_text):
        warnings.append("hybrid_search:WARN:fulltext_fallback")

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
        "warnings": warnings,
    })


@_safe_handler
def search_prior_diagnoses(observation_text: str,
                           charger_id: Optional[str] = None,
                           resolution_filter: str = "any",
                           limit: int = 5) -> str:
    """Find prior investigation outcomes similar to a given observation.
    Filters out superseded diagnoses."""
    vec = embed(observation_text)
    with get_db() as db:
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
    # The model's self-reported `confidence` is telemetry only: it lands in
    # model_confidence, while the decision-grade `confidence` column is
    # derived by the platform. A brand-new checkpoint has no corroboration
    # yet, so it starts at the session base rate.
    derived_confidence = derive_confidence("session", 0, 0)
    with get_db() as db:
        with db.cursor() as cur:
            # Insert the new reasoning
            cur.execute("""
                INSERT INTO agent_reasoning
                    (charger_id, site_id, session_id, observation, hypothesis,
                     evidence_refs, confidence, model_confidence, resolution, tags)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                charger_id, site_id, session_id, observation, hypothesis,
                json.dumps(evidence_refs or []),
                derived_confidence, confidence, resolution,
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
    with get_db() as db:
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

            # Hybrid search with FULLTEXT boost on content (ft_memory_content).
            results, ft_used = _hybrid_search(
                cur, "fleet_memory fm", "fm.memory_vec",
                ["fm.content"], str(vec), query_text,
                where_clauses, params, limit,
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

    # Surface a grep-able sentinel (mirrors the Tier 4/5 pattern in
    # assemble_context) when keywords were present but the full-text
    # query fell back to vector-only.
    warnings = []
    if not ft_used and _extract_keywords(query_text):
        warnings.append("hybrid_search:WARN:fulltext_fallback")

    return to_json({
        "query": query_text[:200],
        "memories": [
            {**{k: v for k, v in r.items()
                if k not in ("memory_vec", "distance", "ft_score")},
             "similarity": round(1 - float(r.get("distance", 1)), 4)}
            for r in results
        ],
        "warnings": warnings,
    })


@_safe_handler
def write_fleet_memory(category: str, scope: str, content: str,
                       source_refs: Optional[list] = None,
                       confidence: float = 0.7,
                       provenance: str = "session",
                       confirmations: int = 0) -> str:
    """Write or merge a fleet memory record.

    UPGRADE: Now checks for semantic contradictions within the same scope
    and auto-supersedes conflicting memories using the superseded_by column.

    Confidence handling: the model-supplied `confidence` is stored into
    model_confidence as telemetry only. The decision-grade `confidence`
    column is DERIVED via derive_confidence() from provenance +
    confirmation/contradiction counters. `provenance`/`confirmations` are
    internal knobs (e.g. consolidation writes 'consolidated' with
    confirmations=len(group)); the agent-facing tool never sets them and
    gets the 'session'/0 defaults.
    """
    vec = embed(content)

    # Look up near-duplicates / candidate contradictions
    with get_db() as db, db.cursor() as cur:
        cur.execute("""
            SELECT id, content, confidence, category,
                   provenance, confirmations, contradictions,
                   VEC_COSINE_DISTANCE(memory_vec, %s) AS distance
            FROM fleet_memory
            WHERE status = 'active' AND scope = %s
            ORDER BY distance ASC
            LIMIT 3
        """, (str(vec), scope))
        nearby = cur.fetchall()

    # Case 1: near-duplicate merge — single UPDATE, atomic under autocommit.
    # A merge is a corroboration event: bump confirmations and RE-DERIVE
    # confidence from the counters. The old max-with-self-report ratchet
    # let a single overconfident self-report permanently raise the score;
    # derive_confidence() only rises with real corroboration and never 1.0.
    if nearby and float(nearby[0]["distance"]) < 0.15:
        closest = nearby[0]
        new_confidence = derive_confidence(
            closest["provenance"],
            int(closest["confirmations"]) + 1,
            int(closest["contradictions"]),
        )
        with get_db() as db, db.cursor() as cur:
            cur.execute("""
                UPDATE fleet_memory
                SET content = %s,
                    confirmations = confirmations + 1,
                    confidence = %s,
                    supporting_evidence_count = supporting_evidence_count + 1,
                    source_refs = JSON_ARRAY_APPEND(
                        COALESCE(source_refs, JSON_ARRAY()), '$', %s
                    ),
                    memory_vec = %s
                WHERE id = %s
            """, (content, new_confidence,
                  json.dumps(source_refs or []),
                  str(vec), closest["id"]))
        return to_json({
            "status": "updated_existing",
            "memory_id": closest["id"],
            "confidence": new_confidence,
            "message": "Near-duplicate found. Merged with existing memory.",
        })

    # Case 2: insert new memory; if it contradicts a prior one, supersede
    # in the same transaction so concurrent readers never see an
    # in-between state and a crash mid-flight rolls back cleanly.
    # Decision-grade confidence is derived; the model self-report rides
    # along in model_confidence as telemetry only.
    derived_confidence = derive_confidence(provenance, confirmations, 0)
    db_conn = _get_pool().connection()
    try:
        db_conn.begin()
        with db_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fleet_memory
                    (category, scope, content, source_refs, confidence,
                     model_confidence, provenance, confirmations,
                     contradictions, memory_vec)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s)
            """, (category, scope, content,
                  json.dumps(source_refs or []),
                  derived_confidence, confidence, provenance, confirmations,
                  str(vec)))
            memory_id = cur.lastrowid

            if (nearby
                    and float(nearby[0]["distance"]) < 0.4
                    and nearby[0]["category"] == category):
                # The prior memory is contradicted by this new one: record a
                # contradiction and re-derive its confidence in the same
                # transaction that supersedes it.
                superseded = nearby[0]
                superseded_confidence = derive_confidence(
                    superseded["provenance"],
                    int(superseded["confirmations"]),
                    int(superseded["contradictions"]) + 1,
                )
                cur.execute("""
                    UPDATE fleet_memory
                    SET status = 'superseded', superseded_by = %s,
                        contradictions = contradictions + 1,
                        confidence = %s
                    WHERE id = %s
                """, (memory_id, superseded_confidence, superseded["id"]))
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        db_conn.close()

    return to_json({
        "status": "created",
        "memory_id": memory_id,
        "confidence": derived_confidence,
        "message": f"Fleet memory recorded (scope: {scope}).",
    })


# Valid real-world outcomes for a verified diagnosis (mirrors the
# agent_reasoning.verified_outcome enum in schema v3).
_VERIFY_OUTCOMES = frozenset({
    "fixed_as_diagnosed", "different_fault", "no_fault_found",
})


@_safe_handler
def verify_outcome(reasoning_id: int, outcome: str,
                   notes: Optional[str] = None) -> str:
    """Record the field-verified real-world outcome of a diagnosis and
    propagate corroboration/contradiction into DERIVED confidence.

    This is the ground-truth signal the whole derived-confidence design
    hinges on. It is DELIBERATELY NOT registered in TOOL_HANDLERS and NOT
    present in tool_definitions.json: the agent must never verify its own
    outcomes — that would recreate exactly the self-report loop this
    refactor removes. Outcomes arrive from a human field tech via
    agent/verify_outcome.py.

    Effects (single transaction):
      * agent_reasoning[reasoning_id].verified_outcome / verified_at set.
      * Every ACTIVE fleet_memory whose source_refs contains
        "agent_reasoning:{reasoning_id}":
          - fixed_as_diagnosed  -> confirmations += 1, and provenance
            promoted session -> verified
          - different_fault / no_fault_found -> contradictions += 1
        then confidence re-derived via derive_confidence().
    """
    if outcome not in _VERIFY_OUTCOMES:
        # Validate before any DB access.
        return to_json({
            "error": f"Invalid outcome {outcome!r}. Must be one of: "
                     f"{sorted(_VERIFY_OUTCOMES)}.",
            "tool": "verify_outcome",
            "retryable": False,
        })

    is_confirmation = outcome == "fixed_as_diagnosed"
    ref = f"agent_reasoning:{reasoning_id}"

    db_conn = _get_pool().connection()
    try:
        db_conn.begin()
        with db_conn.cursor() as cur:
            cur.execute("""
                UPDATE agent_reasoning
                SET verified_outcome = %s, verified_at = NOW()
                WHERE id = %s
            """, (outcome, reasoning_id))
            if cur.rowcount == 0:
                db_conn.rollback()
                return to_json({
                    "error": f"No agent_reasoning row with id {reasoning_id}.",
                    "tool": "verify_outcome",
                    "retryable": False,
                })

            # Fleet memories built from this reasoning chain.
            cur.execute("""
                SELECT id, provenance, confirmations, contradictions
                FROM fleet_memory
                WHERE status = 'active'
                  AND JSON_CONTAINS(source_refs, %s)
            """, (json.dumps(ref),))
            touched = cur.fetchall()

            updated = []
            for row in touched:
                confirmations = int(row["confirmations"])
                contradictions = int(row["contradictions"])
                provenance = row["provenance"]
                if is_confirmation:
                    confirmations += 1
                    if provenance == "session":
                        provenance = "verified"
                else:
                    contradictions += 1
                new_confidence = derive_confidence(
                    provenance, confirmations, contradictions
                )
                cur.execute("""
                    UPDATE fleet_memory
                    SET confirmations = %s, contradictions = %s,
                        provenance = %s, confidence = %s
                    WHERE id = %s
                """, (confirmations, contradictions, provenance,
                      new_confidence, row["id"]))
                updated.append({
                    "memory_id": row["id"],
                    "confidence": new_confidence,
                    "provenance": provenance,
                    "confirmations": confirmations,
                    "contradictions": contradictions,
                })
        db_conn.commit()
    except Exception:
        db_conn.rollback()
        raise
    finally:
        db_conn.close()

    return to_json({
        "status": "ok",
        "reasoning_id": reasoning_id,
        "outcome": outcome,
        "notes": notes,
        "touched_memories": updated,
    })


@_safe_handler
def get_charger_context(entity_id: str,
                        entity_type: str = "charger",
                        snapshot_types: Optional[list] = None) -> str:
    """Retrieve pre-assembled context snapshots for a charger or site."""
    if snapshot_types is None:
        snapshot_types = ["profile", "recent_anomalies"]

    with get_db() as db:
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
        content = _build_snapshot(entity_id, entity_type, snap_type)
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
                    snap_type: str) -> Optional[str]:
    """Build a context snapshot on the fly and cache it."""
    try:
        with get_db() as db, db.cursor() as cur:
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
    with get_db() as db:
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
    with get_db() as db:
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
    """Upsert session state fields.

    Idempotent: if no row exists for session_id, INSERT creates one and
    DB defaults fill columns the caller didn't pass (started_at,
    last_active, token_budget, tokens_used). If the row exists,
    ON DUPLICATE KEY UPDATE applies only the passed columns — same
    semantics as the previous UPDATE-only path. This prevents zero-row
    UPDATEs from silently dropping telemetry like tokens_used when the
    session row hasn't been materialized by an earlier tool call.
    """
    allowed = {"focus_chargers", "focus_site",
               "investigation_summary", "tokens_used"}
    updates = {k: v for k, v in kwargs.items() if k in allowed and v is not None}

    if not updates:
        return to_json({"status": "no_changes"})

    cols = ["session_id"]
    placeholders = ["%s"]
    params = [session_id]
    update_clauses = []
    for k, v in updates.items():
        cols.append(k)
        placeholders.append("%s")
        if k == "focus_chargers":
            params.append(json.dumps(v))
        else:
            params.append(v)
        update_clauses.append(f"{k} = VALUES({k})")

    sql = (
        f"INSERT INTO session_state ({', '.join(cols)}) "
        f"VALUES ({', '.join(placeholders)}) "
        f"ON DUPLICATE KEY UPDATE {', '.join(update_clauses)}"
    )

    with get_db() as db:
        with db.cursor() as cur:
            cur.execute(sql, params)

    return to_json({"status": "updated", "session_id": session_id})


# ============================================================================
# CONTEXT ASSEMBLY (with safety margin)
# ============================================================================

def _summarize_error(err: str) -> str:
    """Compress a tool error message into a short sentinel tag.

    The full message goes to logs; this short form rides in `sources`
    so the model itself can see in its system prompt that retrieval
    was degraded.
    """
    text = (err or "").lower()
    if "meta tensor" in text:
        return "meta_tensor"
    if "database" in text:
        return "database"
    if "timeout" in text or "timed out" in text:
        return "timeout"
    return "unknown"


def assemble_context(session_id: str, charger_id: str,
                     trigger_text: str,
                     token_budget: int = TOKEN_BUDGET_DEFAULT) -> dict:
    """
    Assemble context for Claude's system prompt.

    UPGRADE: Uses effective_budget() with 10% safety margin.

    Returns dict including `top_fleet_match`: the highest-similarity
    fleet_memory entry retrieved by Tier 5, if any. Used by run_agent
    to route high-confidence pattern matches to the Haiku shortcut
    path. Distinct from `sources` which lists which memories made it
    into the seed text — `top_fleet_match` reports what was found,
    even if truncated out of the seed by the R1/R2 caps.
    """
    t_start = time.monotonic()
    sections = []
    sources = []
    tokens_remaining = effective_budget(token_budget)
    top_fleet_match: Optional[dict] = None
    fleet_matches: list[dict] = []

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
        if "error" in similar:
            err_msg = _summarize_error(similar["error"])
            sentinel = f"prior_diagnoses:ERROR:{err_msg}"
            sources.append(sentinel)
            log.warning(
                "assemble_context: Tier 4 (search_prior_diagnoses) "
                "failed for charger %s: %s",
                charger_id, similar["error"],
            )
        else:
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

    # 5. Fleet memory — single query with scope ranking.
    # Parse site_id from the Tier 1 profile snapshot we already fetched
    # rather than re-querying charger_registry.
    #
    # Graceful degradation: if site_id parsing fails (registry missing,
    # snapshot text format changed, etc.) we still run Tier 5 against
    # `scope="any"` rather than skipping the tier entirely. Skipping
    # silently zeros out the routing layer's top_fleet_match, which
    # caused a 0% shortcut rate on a fresh cluster despite a populated
    # fleet_memory. Falling back to "any" preserves cluster recognition
    # even when the data plane is partially incomplete.
    if tokens_remaining > 100:
        site_id = None
        for snap in profile.get("snapshots", []):
            content = snap.get("content", "")
            if " at site " in content:
                site_id = content.split(" at site ", 1)[1].split(".", 1)[0].strip()
                break

        if site_id:
            # Most specific scope: this site's memories OR globals.
            primary_scope = f"site:{site_id}"
        else:
            # Fallback: site unknown, query unconstrained scope. Logged
            # so it's visible when the data plane is in a degraded
            # state rather than silently producing thin seeds.
            primary_scope = "any"
            sources.append("fleet_memory:WARN:site_id_missing")
            log.warning(
                "assemble_context: Tier 5 falling back to scope='any' "
                "for charger %s — no site_id in profile snapshot. "
                "Check that charger_registry has this charger.",
                charger_id,
            )

        # R2: cap at 3 results (was 5). The seed should function as
        # a pointer to fleet patterns, not a content dump — the
        # full memory is one tool call away if the agent needs it.
        # 5 entries inflated the seed as fleet_memory grew across
        # investigations; 3 keeps the per-investigation cost
        # roughly flat against memory size.
        memories = json.loads(recall_fleet_memory(
            trigger_text, scope=primary_scope, limit=3
        ))
        if "error" in memories:
            err_msg = _summarize_error(memories["error"])
            sentinel = f"fleet_memory:ERROR:{err_msg}"
            sources.append(sentinel)
            log.warning(
                "assemble_context: Tier 5 (recall_fleet_memory) "
                "failed for charger %s: %s",
                charger_id, memories["error"],
            )
        else:
            # Capture the matches for the routing layer BEFORE
            # the R1/R2 truncation runs. The seed text may end up
            # smaller (one line, 80-char content) but routing
            # decisions need the full match metadata.
            #
            # We surface BOTH the top-by-similarity match (for
            # legacy callers / telemetry) AND the full ordered
            # list. Routing scans the full list because
            # top-by-similarity isn't always top-by-confidence:
            # a generic high-similarity entry can outrank a more
            # specific high-confidence entry, which would block
            # shortcut routing despite a confident match being
            # available in the same response.
            _all_memories = memories.get("memories", [])
            if _all_memories:
                top_fleet_match = _all_memories[0]
                fleet_matches = _all_memories

            memory_lines = []
            tier5_used = 0  # R1: independent counter for this section
            for m in _all_memories:
                # R2: truncate content to 80 chars (was 150). The
                # seed advertises that a pattern exists; the agent
                # can recall_fleet_memory by id for the full text
                # if a tool call needs it.
                line = (
                    f"- [{m['category']}, {m['scope']}] "
                    f"{m['content'][:80]} "
                    f"(confidence: {m['confidence']}, "
                    f"evidence: {m['supporting_evidence_count']}x)"
                )
                line_tokens = count_tokens(line)
                # R1: gate on BOTH the global remaining budget AND
                # the Tier 5 hard cap. Either limit terminates the
                # loop. Order matters for correctness — once the
                # cap is hit, subsequent (less relevant) entries
                # are skipped even if global budget would allow them.
                if (line_tokens <= tokens_remaining
                        and tier5_used + line_tokens <= TIER5_MAX_TOKENS):
                    memory_lines.append(line)
                    tokens_remaining -= line_tokens
                    tier5_used += line_tokens
                    sources.append(f"fleet_memory:{m['id']}")
                else:
                    break  # respect the cap; don't keep scanning
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
        "top_fleet_match": top_fleet_match,
        "fleet_matches": fleet_matches,
    }


# ============================================================================
# LIFECYCLE JOBS
# ============================================================================

def consolidation_job():
    """Promote confirmed reasoning patterns into fleet memory."""
    import anthropic
    client = anthropic.Anthropic()

    with get_db() as db, db.cursor() as cur:
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
                fallback_content = (
                    f"Confirmed pattern across {len(group)} chargers "
                    f"({', '.join(charger_ids[:5])}): "
                    f"{sample['hypothesis'] or sample['observation']}. "
                    f"Tags: {tag_key}."
                )
                # Synthesize a cleaner natural-language description via Haiku.
                # On any failure (rate limit, network, empty response) we fall
                # back to the deterministic f-string — consolidation must not
                # break on transient API errors.
                try:
                    group_summary = "\n".join(
                        f"- {g['charger_id']}: observed {g['observation']!r}; "
                        f"hypothesis {g['hypothesis']!r}"
                        for g in group[:10]
                    )
                    response = client.messages.create(
                        model=HAIKU_MODEL,
                        max_tokens=200,
                        system=(
                            "You write concise pattern descriptions for an EV "
                            "charger fleet memory store. Given a group of "
                            "confirmed reasoning entries that share tags, "
                            "synthesize a 2-3 sentence pattern statement that "
                            "names the failure mode, references the number of "
                            "affected chargers, and notes any common cause. "
                            "Be specific and operational. Do not use bullet "
                            "points or markdown."
                        ),
                        messages=[{
                            "role": "user",
                            "content": (
                                f"Group of {len(group)} confirmed entries "
                                f"sharing tags '{tag_key}':\n"
                                f"{group_summary}\n\n"
                                "Write a 2-3 sentence pattern description."
                            ),
                        }],
                    )
                    log.info(
                        "consolidation api_call: model=%s tag_key=%r "
                        "input_tokens=%d output_tokens=%d",
                        HAIKU_MODEL, tag_key,
                        response.usage.input_tokens,
                        response.usage.output_tokens,
                    )
                    synth = "".join(
                        b.text for b in response.content if b.type == "text"
                    ).strip()
                    content = synth if synth else fallback_content
                except Exception as e:
                    log.warning(
                        f"Haiku pattern synthesis failed for tag_key={tag_key!r}: {e}; "
                        f"using f-string fallback"
                    )
                    content = fallback_content
                environments = {g["environment"] for g in group if g.get("environment")}
                models = {f"{g['manufacturer']}-{g['model']}" for g in group
                          if g.get("manufacturer")}
                if len(models) == 1:
                    scope = f"model:{models.pop()}"
                elif len(environments) == 1:
                    scope = f"environment:{environments.pop()}"
                else:
                    scope = "global"

                # Confidence is NOT hardcoded: it falls out of
                # derive_confidence('consolidated', len(group), 0). A group
                # of N confirmed cases IS the corroboration.
                write_fleet_memory(
                    category="pattern", scope=scope, content=content,
                    source_refs=[f"agent_reasoning:{g['id']}" for g in group],
                    provenance="consolidated", confirmations=len(group),
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
    with get_db() as db, db.cursor() as cur:
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

        # Confidence decay: 5% reduction per month. Gate on last_decayed_at,
        # NOT updated_at — access-count writes bump updated_at, which silently
        # blocked decay for hot memories. A NULL clock means never decayed, so
        # fall back to created_at for the first decay.
        cur.execute("""
            UPDATE fleet_memory
            SET confidence = ROUND(confidence * 0.95, 2),
                last_decayed_at = NOW()
            WHERE status = 'active'
              AND (
                    (last_decayed_at IS NULL
                     AND created_at < NOW() - INTERVAL 30 DAY)
                 OR last_decayed_at < NOW() - INTERVAL 30 DAY
              )
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
    merged = 0
    with get_db() as db, db.cursor() as cur:
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
    with get_db() as db, db.cursor() as cur:
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
            _build_snapshot(cid, "charger", snap_type)
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
    try:
        return handler(**tool_input)
    except TypeError as e:
        # Schema-level mismatch: e.g. model omitted a required arg or
        # passed an unexpected one. Bubble up a structured error so the
        # agent can correct itself instead of crashing the wrapper.
        log.warning(
            "Tool input validation failed for %s with input keys %s: %s",
            tool_name, list(tool_input.keys()), e,
        )
        return json.dumps({
            "error": f"Invalid arguments for {tool_name}: {e}",
            "tool": tool_name,
            "retryable": True,
            "provided_args": list(tool_input.keys()),
        })


# ============================================================================
# AGENT LOOP (with circuit breaker + observability)
# ============================================================================

# Model identifiers used across this module. Sonnet handles open-ended
# investigations; Haiku handles classification and summarization.
SONNET_MODEL = "claude-sonnet-4-6"
HAIKU_MODEL = "claude-haiku-4-5-20251001"

# SHORTCUT routing gate thresholds. The confidence here is the platform
# DERIVED value (Beta-posterior over verified outcomes), so thresholding it
# is sound — unlike the old model self-report, an inflated number no longer
# buys cheaper Haiku processing.
SHORTCUT_CONFIDENCE_MIN = 0.85
SHORTCUT_SIMILARITY_MIN = 0.55
SHORTCUT_CONFIRMATIONS_MIN = 3


def _shortcut_eligible(match: dict) -> bool:
    """Structural gate deciding if a fleet_memory match may fire the Haiku
    SHORTCUT route. Pure function of the match dict so it is unit-testable.

    Requires ALL of:
      * derived confidence >= SHORTCUT_CONFIDENCE_MIN (0.85)
      * confirmations >= SHORTCUT_CONFIRMATIONS_MIN (3) OR provenance
        == 'verified' — i.e. real corroboration, not a lone high number
      * not superseded (superseded_by IS NULL — the match came back active,
        already implied by recall's WHERE, asserted here regardless)
      * similarity >= SHORTCUT_SIMILARITY_MIN (0.55)
    """
    if match.get("superseded_by") is not None:
        return False
    try:
        conf = float(match.get("confidence", 0))
        sim = float(match.get("similarity", 0))
        confirmations = int(match.get("confirmations", 0))
    except (TypeError, ValueError):
        return False
    provenance = match.get("provenance")
    if conf < SHORTCUT_CONFIDENCE_MIN:
        return False
    if not (confirmations >= SHORTCUT_CONFIRMATIONS_MIN
            or provenance == "verified"):
        return False
    if sim < SHORTCUT_SIMILARITY_MIN:
        return False
    return True


def _classify_trigger(trigger: str, client, obs: Optional[AgentObserver] = None) -> str:
    """Classify trigger as 'lookup' or 'investigation'.
    Simple lookups (status checks, profile queries) can be answered
    by Haiku alone. Investigations need Sonnet + tool loop."""
    response = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=20,
        system=(
            "Classify the user's request as exactly one word: "
            "'lookup' if it's a simple status/profile/history query "
            "answerable from one or two tool calls, or 'investigation' "
            "if it requires hypothesis formation, anomaly analysis, or "
            "cross-referencing multiple data sources. Respond with one word."
        ),
        messages=[{"role": "user", "content": trigger}],
    )
    if obs is not None:
        obs.record_api_call(
            role="classify",
            model=HAIKU_MODEL,
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
        )
    text = "".join(b.text for b in response.content if b.type == "text").strip().lower()
    return "lookup" if "lookup" in text else "investigation"


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

    # Routing decision. Three paths, evaluated in priority order:
    #
    # 1. SHORTCUT — Tier 5 of assemble_context found a high-confidence
    #    fleet_memory match (confidence >= 0.95 AND similarity >= 0.85).
    #    The cognitive foundation has already seen this pattern, so we
    #    skip expensive Sonnet reasoning and route to Haiku for verify-
    #    and-checkpoint in 3 rounds. Skips the classify call too —
    #    we already know the answer shape.
    #
    # 2. LOOKUP — Haiku classifier reads only the trigger string and
    #    flags simple status queries ("what's the status of X?") that
    #    can be answered in 1-2 tool calls. Haiku + 5 rounds.
    #
    # 3. EXPLORE — default path. No high-confidence match, not a
    #    lookup-shape trigger. Sonnet + 15 rounds (the legacy path).
    # Scan ALL Tier 5 matches, not just the top-by-similarity one.
    # The most-similar entry isn't always the most-confident — a
    # generic 0.72-confidence pattern with rich vocabulary overlap
    # can outrank a charger-specific 0.97-confidence entry in cosine
    # space. We want shortcut to fire whenever ANY returned match
    # passes the structural gate, preferring the highest DERIVED
    # confidence among those that do.
    #
    # The gate is now STRUCTURAL (_shortcut_eligible): high derived
    # confidence is necessary but not sufficient — it also requires real
    # corroboration (confirmations >= 3 or verified provenance) and that
    # the match is not superseded. A self-reported number can no longer
    # buy the cheaper Haiku route.
    top_match = ctx.get("top_fleet_match")  # kept for telemetry
    fleet_matches = ctx.get("fleet_matches") or []

    best_shortcut_match: Optional[dict] = None
    for m in fleet_matches:
        if not _shortcut_eligible(m):
            continue
        try:
            _conf = float(m.get("confidence", 0))
        except (TypeError, ValueError):
            continue
        if (best_shortcut_match is None
                or _conf > float(best_shortcut_match.get("confidence", 0))):
            best_shortcut_match = m

    # Print routing inputs directly to stderr. log.info goes to a
    # logger that has no handler attached when dispatch.py runs and
    # gets silently dropped at default WARNING level, so we bypass
    # Python logging here for guaranteed visibility. Tagged [ROUTING]
    # so it's grep-friendly.
    import sys as _sys_routing
    _top_str = (
        f"id={top_match.get('id')}"
        f"/conf={top_match.get('confidence')}"
        f"/sim={top_match.get('similarity')}"
        if top_match else "None"
    )
    _eligible_str = (
        f"id={best_shortcut_match.get('id')}"
        f"/conf={best_shortcut_match.get('confidence')}"
        f"/confirm={best_shortcut_match.get('confirmations')}"
        f"/contra={best_shortcut_match.get('contradictions')}"
        f"/prov={best_shortcut_match.get('provenance')}"
        f"/sim={best_shortcut_match.get('similarity')}"
        if best_shortcut_match else "none"
    )
    _sys_routing.stderr.write(
        f"[ROUTING] session={session_id} matches={len(fleet_matches)} "
        f"top_match={_top_str} eligible={_eligible_str}\n"
    )
    _sys_routing.stderr.flush()

    if best_shortcut_match is not None:
        loop_model = HAIKU_MODEL
        max_tool_rounds = 3
        routing_signal = "shortcut"
        obs.record_routing(routing_signal, best_shortcut_match)
        log.info(
            "Routing: SHORTCUT for session %s — fleet_memory id=%s "
            "confidence=%s similarity=%s; loop_model=%s, max_rounds=%d",
            session_id, best_shortcut_match.get("id"),
            best_shortcut_match.get("confidence"),
            best_shortcut_match.get("similarity"),
            loop_model, max_tool_rounds,
        )
    else:
        # Fall through to the existing classifier.
        classification = _classify_trigger(trigger, client, obs)
        if classification == "lookup":
            loop_model = HAIKU_MODEL
            max_tool_rounds = 5
            routing_signal = "lookup"
        else:
            loop_model = SONNET_MODEL
            max_tool_rounds = 15
            routing_signal = "explore"
        obs.record_routing(routing_signal, top_match)
        log.info(
            f"Routing: {routing_signal.upper()} for session {session_id}: "
            f"model={loop_model}, max_tool_rounds={max_tool_rounds}"
        )

    messages = [{"role": "user", "content": trigger}]

    tool_call_counts: dict[str, int] = {}
    last_tools: list[str] = []  # Track consecutive tool calls

    # Step 4: Agent loop with circuit breaker.
    # The system prompt is byte-identical across all iterations of this
    # investigation, so we cache it. Render order is tools → system →
    # messages; cache_control on the system block caches both tools and
    # system together. Iteration 1 pays the ~1.25x write premium;
    # iterations 2..N read at ~0.1x, which dominates the saving on long
    # loops. The system prompt comfortably exceeds Sonnet 4.6's 2048-
    # token cache minimum once context is assembled.
    cached_system = [{
        "type": "text",
        "text": system_prompt,
        "cache_control": {"type": "ephemeral"},
    }]

    for iteration in range(1, max_tool_rounds + 1):
        response = client.messages.create(
            model=loop_model,
            max_tokens=2048,
            system=cached_system,
            tools=tools_config["tools"],
            messages=messages,
        )
        obs.record_api_call(
            role="loop",
            model=loop_model,
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            cache_creation_input_tokens=getattr(
                response.usage, "cache_creation_input_tokens", 0
            ) or 0,
            cache_read_input_tokens=getattr(
                response.usage, "cache_read_input_tokens", 0
            ) or 0,
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

    # Force a clean final summary via Haiku, but DO NOT replay the full
    # `messages` array. The loop already wrote a structured checkpoint
    # to agent_reasoning; re-derive the report from that row instead.
    # Replaying messages cost ~30-50k input tokens per investigation
    # (the whole conversation, growing with each iteration) and Haiku
    # frequently returned zero text blocks when given that bloated,
    # mixed-content payload — the empty-Investigation-Report bug.
    checkpoint = None
    try:
        with get_db() as db, db.cursor() as cur:
            cur.execute(
                """
                SELECT id, charger_id, observation, hypothesis,
                       evidence_refs, confidence, resolution, tags,
                       created_at
                FROM agent_reasoning
                WHERE session_id = %s
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (session_id,),
            )
            checkpoint = cur.fetchone()
    except Exception as e:
        log.warning(
            "Could not fetch checkpoint for session %s: %s", session_id, e
        )

    if checkpoint:
        summary_prompt = (
            f"Write a final investigation report for charger {charger_id}.\n\n"
            f"Original alert: {trigger}\n\n"
            "Investigation checkpoint:\n"
            f"  Observation: {checkpoint['observation']}\n"
            f"  Hypothesis: {checkpoint.get('hypothesis') or '(no hypothesis)'}\n"
            f"  Confidence: {checkpoint['confidence']}\n"
            f"  Resolution: {checkpoint['resolution']}\n"
            f"  Evidence: {checkpoint.get('evidence_refs') or '[]'}\n"
            f"  Tags: {checkpoint.get('tags') or '[]'}\n"
            f"  Reasoning ID: {checkpoint['id']}\n\n"
            "Write a 3-5 paragraph report covering: what was observed, "
            "the diagnosed cause, the recommended action, and any "
            "fleet-wide implications. Reference the evidence and the "
            "reasoning ID."
        )
    else:
        # Fallback: no checkpoint was written (rare — circuit breaker
        # fired before any write_reasoning_checkpoint call, or the model
        # never reached a checkpoint). Pull the last assistant text
        # block from the loop so the report still reflects what the
        # agent was reasoning about.
        last_text = ""
        for msg in reversed(messages):
            if msg.get("role") != "assistant":
                continue
            content = msg.get("content")
            if isinstance(content, str):
                last_text = content
                break
            if not content:
                continue
            for block in content:
                block_type = getattr(block, "type", None) or (
                    block.get("type") if isinstance(block, dict) else None
                )
                if block_type != "text":
                    continue
                block_text = getattr(block, "text", None) or (
                    block.get("text") if isinstance(block, dict) else None
                )
                if block_text:
                    last_text = block_text
                    break
            if last_text:
                break

        summary_prompt = (
            f"Write a final investigation report for charger {charger_id}.\n\n"
            f"Original alert: {trigger}\n\n"
            "No reasoning checkpoint was written for this session — the "
            "investigation may have been interrupted. Partial reasoning "
            f"from the agent:\n\n{last_text or '(no reasoning text captured)'}\n\n"
            "Write a 2-3 paragraph report describing what was "
            "investigated and what remains uncertain or unfinished."
        )

    summary_response = client.messages.create(
        model=HAIKU_MODEL,
        max_tokens=1024,
        system=(
            "You are an EV charger fleet diagnostic report writer. "
            "Produce concise, operational reports for field technicians."
        ),
        messages=[{"role": "user", "content": summary_prompt}],
    )
    obs.record_api_call(
        role="summary",
        model=HAIKU_MODEL,
        input_tokens=summary_response.usage.input_tokens,
        output_tokens=summary_response.usage.output_tokens,
    )
    final_text = "\n".join(
        b.text for b in summary_response.content if b.type == "text"
    )

    # Update session. tokens_used is the total Anthropic API tokens
    # (input + output) consumed by this investigation, not the size of
    # the assembled prompt — that lives in `context_tokens` in the
    # return dict.
    api_total_tokens = obs.api_input_tokens + obs.api_output_tokens
    update_session_state(
        session_id,
        focus_chargers=[charger_id],
        investigation_summary=final_text[:500],
        tokens_used=api_total_tokens,
    )

    obs.agent_complete()

    return {
        "response": final_text,
        "tool_calls": tool_call_counts,
        "context_sources": ctx["sources"],
        "context_tokens": ctx["tokens_used"],
        "api_input_tokens": obs.api_input_tokens,
        "api_output_tokens": obs.api_output_tokens,
        "api_calls_by_role": dict(obs.api_calls_by_role),
        "observability": obs.summary(),
    }
