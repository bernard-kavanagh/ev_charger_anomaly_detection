"""
EV Charger IoT Agent — Tool Handler Backend
============================================
Implements all Claude tool definitions against a single TiDB Cloud cluster.
This is the "behaviour layer" that replaces db9/mem9 with pure TiDB.

Dependencies:
    pip install pymysql anthropic tiktoken

Environment variables:
    TIDB_HOST, TIDB_PORT, TIDB_USER, TIDB_PASSWORD, TIDB_DATABASE
    ANTHROPIC_API_KEY  (for embedding generation)
"""

import json
import os
import hashlib
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pymysql
import tiktoken

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
# Matches embedding_service.py and the VECTOR(384) schema columns.
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
# Token counting (for context budget management)
# ---------------------------------------------------------------------------

_enc = tiktoken.encoding_for_model("gpt-4")  # close enough for Claude token estimates

def count_tokens(text: str) -> int:
    return len(_enc.encode(text))


# ---------------------------------------------------------------------------
# JSON serialisation helper (handles Decimal, datetime)
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


# ============================================================================
# TOOL HANDLERS
# ============================================================================

def search_similar_outages(window_id: int, limit: int = 5,
                           severity_filter: str = "any",
                           category_filter: str = "any") -> str:
    """Find outage catalog entries similar to a given anomaly window."""
    db = get_db()
    with db.cursor() as cur:
        # Step 1: Get the window's signature vector
        cur.execute(
            "SELECT signature_vec, charger_id, window_start, anomaly_score "
            "FROM charger_windows WHERE id = %s", (window_id,)
        )
        window = cur.fetchone()
        if not window or not window["signature_vec"]:
            return to_json({"error": f"Window {window_id} not found or has no embedding."})

        # Step 2: Vector similarity search against outage catalog
        where_clauses = []
        params = []
        if severity_filter != "any":
            where_clauses.append("oc.severity = %s")
            params.append(severity_filter)
        if category_filter != "any":
            where_clauses.append("oc.category = %s")
            params.append(category_filter)

        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        sql = f"""
            SELECT oc.pattern_id, oc.pattern_name, oc.category,
                   oc.root_cause, oc.symptoms, oc.resolution, oc.severity,
                   oc.occurrence_count,
                   VEC_COSINE_DISTANCE(oc.signature_vec, %s) AS distance
            FROM outage_catalog oc
            {where_sql}
            ORDER BY distance ASC
            LIMIT %s
        """
        params = [window["signature_vec"]] + params + [limit]
        cur.execute(sql, params)
        results = cur.fetchall()

    return to_json({
        "window": {
            "id": window_id,
            "charger_id": window["charger_id"],
            "window_start": window["window_start"],
            "anomaly_score": window["anomaly_score"],
        },
        "similar_outages": [
            {**r, "similarity": round(1 - float(r["distance"]), 4)}
            for r in results
        ],
    })


def search_prior_diagnoses(observation_text: str,
                           charger_id: Optional[str] = None,
                           resolution_filter: str = "any",
                           limit: int = 5) -> str:
    """Find prior investigation outcomes similar to a given observation."""
    vec = embed(observation_text)
    db = get_db()
    with db.cursor() as cur:
        where_clauses = ["ar.reasoning_vec IS NOT NULL"]
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
            {**r, "similarity": round(1 - float(r["distance"]), 4)}
            for r in results
        ],
    })


def write_reasoning_checkpoint(session_id: str, charger_id: str,
                                observation: str,
                                site_id: Optional[str] = None,
                                hypothesis: Optional[str] = None,
                                evidence_refs: Optional[list] = None,
                                confidence: float = 0.5,
                                resolution: str = "confirmed",
                                tags: Optional[list] = None) -> str:
    """Write a reasoning checkpoint to the agent_reasoning table."""
    db = get_db()
    with db.cursor() as cur:
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
        reasoning_id = cur.lastrowid

    return to_json({
        "status": "ok",
        "reasoning_id": reasoning_id,
        "message": f"Checkpoint saved for {charger_id}.",
    })


def recall_fleet_memory(query_text: str,
                        scope: str = "any",
                        category_filter: str = "any",
                        limit: int = 5) -> str:
    """Retrieve relevant fleet knowledge by vector similarity + filters."""
    vec = embed(query_text)
    db = get_db()
    with db.cursor() as cur:
        where_clauses = ["fm.status = 'active'"]
        params = [str(vec)]
        if scope != "any":
            # Match exact scope OR global
            where_clauses.append("(fm.scope = %s OR fm.scope = 'global')")
            params.append(scope)
        if category_filter != "any":
            where_clauses.append("fm.category = %s")
            params.append(category_filter)

        where_sql = "WHERE " + " AND ".join(where_clauses)

        sql = f"""
            SELECT fm.id, fm.category, fm.scope, fm.content,
                   fm.confidence, fm.supporting_evidence_count,
                   fm.created_at, fm.updated_at,
                   VEC_COSINE_DISTANCE(fm.memory_vec, %s) AS distance
            FROM fleet_memory fm
            {where_sql}
            ORDER BY distance ASC
            LIMIT %s
        """
        params.append(limit)
        cur.execute(sql, params)
        results = cur.fetchall()

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
            {**r, "similarity": round(1 - float(r["distance"]), 4)}
            for r in results
        ],
    })


def write_fleet_memory(category: str, scope: str, content: str,
                       source_refs: Optional[list] = None,
                       confidence: float = 0.7) -> str:
    """Write a new fleet memory record."""
    vec = embed(content)
    db = get_db()
    with db.cursor() as cur:
        # Check for near-duplicate (similarity > 0.92)
        cur.execute("""
            SELECT id, content,
                   VEC_COSINE_DISTANCE(memory_vec, %s) AS distance
            FROM fleet_memory
            WHERE status = 'active' AND scope = %s
            ORDER BY distance ASC
            LIMIT 1
        """, (str(vec), scope))
        existing = cur.fetchone()

        if existing and float(existing["distance"]) < 0.15:
            # Very similar memory exists — update it instead
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
                  str(vec), existing["id"]))
            return to_json({
                "status": "updated_existing",
                "memory_id": existing["id"],
                "message": "Near-duplicate found. Merged with existing memory.",
            })

        # Insert new memory
        cur.execute("""
            INSERT INTO fleet_memory
                (category, scope, content, source_refs, confidence, memory_vec)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (category, scope, content,
              json.dumps(source_refs or []),
              confidence, str(vec)))
        memory_id = cur.lastrowid

    return to_json({
        "status": "created",
        "memory_id": memory_id,
        "message": f"Fleet memory recorded (scope: {scope}).",
    })


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

    # If snapshots are missing, build them on the fly
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
                    lines.append(
                        f"  {w['window_start']}: score={w['anomaly_score']}, "
                        f"power={w['avg_power_w']}W, volt_std={w['voltage_stddev']}, "
                        f"temp={w['max_temp_c']}C, errors={w['error_count']}, "
                        f"flags={w['anomaly_flags']}"
                    )
                content = "\n".join(lines)

        elif entity_type == "charger" and snap_type == "active_investigations":
            cur.execute("""
                SELECT observation, hypothesis, confidence, resolution, created_at
                FROM agent_reasoning
                WHERE charger_id = %s AND resolution = 'open'
                ORDER BY created_at DESC
                LIMIT 5
            """, (entity_id,))
            investigations = cur.fetchall()
            if not investigations:
                content = f"No open investigations for {entity_id}."
            else:
                lines = [f"Open investigations for {entity_id}:"]
                for inv in investigations:
                    lines.append(
                        f"  [{inv['created_at']}] {inv['observation']} "
                        f"→ {inv['hypothesis'] or 'no hypothesis yet'} "
                        f"(confidence: {inv['confidence']})"
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


def get_recent_windows(charger_id: str, hours_back: int = 6,
                       anomaly_only: bool = False) -> str:
    """Retrieve recent window aggregates for a charger."""
    db = get_db()
    with db.cursor() as cur:
        anomaly_clause = "AND anomaly_score > 0" if anomaly_only else ""
        cur.execute(f"""
            SELECT id, window_start, window_end, msg_count,
                   avg_power_w, max_power_w, min_voltage_v, max_voltage_v,
                   voltage_stddev, avg_current_a, max_temp_c, avg_temp_c,
                   error_count, status_changes, distinct_errors,
                   avg_fan_rpm, max_earth_leak, anomaly_flags, anomaly_score
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
                "token_budget": 4000,
                "tokens_used": 0,
            }

    return to_json(session)


def update_session_state(session_id: str, **kwargs) -> str:
    """Update session state fields."""
    db = get_db()
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

    with get_db().cursor() as cur:
        cur.execute(
            f"UPDATE session_state SET {', '.join(set_clauses)} "
            f"WHERE session_id = %s", params
        )

    return to_json({"status": "updated", "session_id": session_id})


# ============================================================================
# CONTEXT ASSEMBLY — The function that builds Claude's prompt
# ============================================================================

def assemble_context(session_id: str, charger_id: str,
                     trigger_text: str,
                     token_budget: int = 4000) -> dict:
    """
    Assemble the context that gets injected into Claude's system prompt.
    
    This is the key function that replaces what mem9/db9's "auto-recall"
    does automatically. It gathers relevant information from all context
    plane tables and assembles a prompt that fits within the token budget.
    
    Returns:
        {
            "system_context": str,   # inject into Claude's system prompt
            "tokens_used": int,
            "sources": list[str],    # what was included, for auditing
        }
    """
    sections = []
    sources = []
    tokens_remaining = token_budget

    # 1. Charger profile (always include, cheap ~80 tokens)
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
            sections.append(f"## Open investigations\n{snap['content']}")
            tokens_remaining -= snap["token_count"]
            sources.append("context_snapshot:active_investigations")

    # 4. Prior confirmed diagnoses (~200-500 tokens, most valuable)
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

    # 5. Fleet memory (~100-300 tokens)
    if tokens_remaining > 100:
        # Get charger's registry to determine scope
        db = get_db()
        with db.cursor() as cur:
            cur.execute(
                "SELECT site_id, manufacturer, model, environment "
                "FROM charger_registry WHERE charger_id = %s",
                (charger_id,)
            )
            reg = cur.fetchone()

        if reg:
            scopes_to_try = [
                f"site:{reg['site_id']}",
                f"model:{reg['manufacturer']}-{reg['model']}",
                "global",
            ]
            memory_lines = []
            seen_ids = set()
            for scope in scopes_to_try:
                if tokens_remaining < 50:
                    break
                memories = json.loads(recall_fleet_memory(
                    trigger_text, scope=scope, limit=2
                ))
                for m in memories.get("memories", []):
                    if m["id"] in seen_ids:
                        continue
                    seen_ids.add(m["id"])
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
    tokens_used = token_budget - tokens_remaining

    return {
        "system_context": system_context,
        "tokens_used": tokens_used,
        "sources": sources,
    }


# ============================================================================
# LIFECYCLE JOBS — Consolidation, cleanup, snapshot refresh
# ============================================================================

def consolidation_job():
    """
    Periodic job (run hourly or daily) that promotes confirmed reasoning
    patterns into fleet memory. This is the "memory lifecycle" logic.
    
    Schedule via cron, Flink batch job, or a simple scheduler.
    """
    db = get_db()
    with db.cursor() as cur:
        # Find reasoning chains that were confirmed and not yet promoted
        cur.execute("""
            SELECT ar.charger_id, ar.site_id, ar.observation, 
                   ar.hypothesis, ar.tags, ar.id,
                   cr.manufacturer, cr.model, cr.environment
            FROM agent_reasoning ar
            LEFT JOIN charger_registry cr ON ar.charger_id = cr.charger_id
            WHERE ar.resolution = 'confirmed'
              AND ar.created_at > NOW() - INTERVAL 7 DAY
        """)
        confirmed = cur.fetchall()

        # Group by hypothesis similarity to find patterns
        # (simplified: in production, cluster by embedding similarity)
        tag_groups = {}
        for r in confirmed:
            tags = json.loads(r["tags"]) if r["tags"] else []
            key = "|".join(sorted(tags)) if tags else "untagged"
            tag_groups.setdefault(key, []).append(r)

        promoted = 0
        for tag_key, group in tag_groups.items():
            if len(group) >= 3:
                # 3+ confirmed cases with the same tags = a pattern
                charger_ids = [g["charger_id"] for g in group]
                sample = group[0]
                content = (
                    f"Confirmed pattern across {len(group)} chargers "
                    f"({', '.join(charger_ids[:5])}): "
                    f"{sample['hypothesis'] or sample['observation']}. "
                    f"Tags: {tag_key}."
                )
                # Determine scope
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
                    category="pattern",
                    scope=scope,
                    content=content,
                    source_refs=[f"agent_reasoning:{g['id']}" for g in group],
                    confidence=0.8,
                )

                # Mark as promoted
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
    Periodic cleanup: expire old sessions, prune stale snapshots.
    Schedule daily.
    """
    db = get_db()
    with db.cursor() as cur:
        # Expire inactive sessions (>24h)
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

        # Deprecate fleet memories with 0 access in 90 days
        cur.execute("""
            UPDATE fleet_memory
            SET status = 'deprecated'
            WHERE status = 'active'
              AND access_count = 0
              AND created_at < NOW() - INTERVAL 90 DAY
        """)
        memories_deprecated = cur.rowcount

    return (
        f"Cleanup: {sessions_cleaned} sessions expired, "
        f"{snapshots_cleaned} snapshots pruned, "
        f"{memories_deprecated} memories deprecated."
    )


def refresh_snapshots_job(charger_ids: Optional[list] = None):
    """
    Refresh context snapshots for chargers with recent activity.
    Schedule every 15-30 minutes.
    """
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
            # Refresh snapshots for chargers with recent anomalies
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
# TOOL DISPATCHER — Route Claude's tool calls to handlers
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
# EXAMPLE: Full agent invocation loop
# ============================================================================

def run_agent(session_id: str, user_id: str, charger_id: str,
              trigger: str) -> dict:
    """
    Complete agent invocation:
    1. Assemble context from TiDB
    2. Call Claude with tools
    3. Handle tool calls in a loop
    4. Return dict with response, tool_call_counts, context metadata
    """
    import anthropic

    # Step 1: Assemble context
    ctx = assemble_context(session_id, charger_id, trigger)

    # Step 2: Load tool definitions
    _tools_path = Path(__file__).parent / "tool_definitions.json"
    with open(_tools_path) as f:
        tools_config = json.load(f)

    # Step 3: Build the system prompt
    system_prompt = f"""You are an EV charger fleet diagnostic agent. You analyse
IoT telemetry data from 20,000 electric vehicle charge points to detect outages,
service degradation, and failure patterns.

You have access to tools for searching similar outages, retrieving and writing
reasoning checkpoints, and recalling fleet-wide knowledge. Always checkpoint
your reasoning after reaching a hypothesis. Promote confirmed patterns to
fleet memory.

Current session: {session_id}
Current charger: {charger_id}

{ctx['system_context']}

Context sources: {', '.join(ctx['sources'])}
Tokens used for context: {ctx['tokens_used']}/4000
"""

    client = anthropic.Anthropic()
    messages = [{"role": "user", "content": trigger}]
    tool_call_counts: dict[str, int] = {}

    # Step 4: Agent loop (handle tool calls)
    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system_prompt,
            tools=tools_config["tools"],
            messages=messages,
        )

        # Collect response content
        assistant_content = response.content
        messages.append({"role": "assistant", "content": assistant_content})

        # Check if there are tool calls to handle
        tool_calls = [b for b in assistant_content if b.type == "tool_use"]
        if not tool_calls:
            break  # No more tool calls, agent is done

        # Handle each tool call
        tool_results = []
        for tc in tool_calls:
            tool_call_counts[tc.name] = tool_call_counts.get(tc.name, 0) + 1
            result = handle_tool_call(tc.name, tc.input)
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tc.id,
                "content": result,
            })
        messages.append({"role": "user", "content": tool_results})

    # Extract final text response
    final_text = "\n".join(
        b.text for b in assistant_content if b.type == "text"
    )

    # Update session state
    update_session_state(
        session_id,
        focus_chargers=[charger_id],
        investigation_summary=final_text[:500],
        tokens_used=ctx["tokens_used"],
    )

    return {
        "response": final_text,
        "tool_calls": tool_call_counts,
        "context_sources": ctx["sources"],
        "context_tokens": ctx["tokens_used"],
    }
