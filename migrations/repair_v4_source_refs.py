#!/usr/bin/env python3
"""
v4 data repair — flatten source_refs, then backfill the split counters
======================================================================
Two-phase, ordered-by-construction repair for the v4 counter split. Run this
AFTER applying migrations/schema_v4_counter_split.sql (which adds the
verified_confirmations / verified_contradictions / corroborations /
supersede_events columns) and BEFORE relying on the new derived confidence.

  Phase 1 — source_refs flattening
      The pre-v4 merge path appended json.dumps(list) via JSON_ARRAY_APPEND,
      producing nested arrays like [..., ["agent_reasoning:X"]]. Those nested
      refs are invisible to verify_outcome's JSON_CONTAINS on a top-level
      scalar. Phase 1 recursively flattens every fleet_memory.source_refs into
      a deduped flat list of scalar strings matching ^[a-z_]+:\\d+$, dropping
      malformed elements. Each row is repaired inside its own transaction with
      SELECT ... FOR UPDATE.

  Phase 2 — counter backfill (depends on phase 1)
      Recomputes, per fleet_memory row:
        verified_confirmations  = COUNT(DISTINCT linked agent_reasoning rows
                                  whose verified_outcome = 'fixed_as_diagnosed')
        verified_contradictions = COUNT(DISTINCT linked rows whose
                                  verified_outcome IN
                                  ('different_fault','no_fault_found'))
        corroborations          = OLD confirmations   (frozen history)
        supersede_events        = OLD contradictions  (frozen history)
        confidence              = derive_confidence(provenance,
                                                    verified_confirmations,
                                                    verified_contradictions)
      The verified counters are recomputed PURELY from verified_outcome — the
      polluted pre-v4 confirmations/contradictions are NOT max'd in.

Idempotent: a second run flattens nothing (refs already flat) and recomputes
identical counters, so it reports no changes.

Usage:
    python migrations/repair_v4_source_refs.py --dry-run   # review, no writes
    python migrations/repair_v4_source_refs.py             # apply (both phases)

Do NOT run this from Claude Code against the live cluster. Bernard runs it
manually against cluster 2 after reviewing the --dry-run output.
"""

import argparse
import os
import sys
from pathlib import Path

# Load .env before importing tool_handlers so all env vars are present.
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Add project root to sys.path so `import tool_handlers` works regardless of CWD.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pymysql  # noqa: E402
from tool_handlers import (  # noqa: E402  (after sys.path patch)
    derive_confidence,
    _flatten_source_refs,
    _SOURCE_REF_RE,
    _AGENT_REASONING_REF_RE,
)

import json  # noqa: E402


def _connect():
    """Open a direct pymysql connection from the same env vars as the pool.

    autocommit is OFF: this script drives explicit per-row transactions so it
    can hold SELECT ... FOR UPDATE row locks across the read-modify-write.
    """
    ssl_ca = os.environ.get("TIDB_SSL_CA")
    return pymysql.connect(
        host=os.environ["TIDB_HOST"],
        port=int(os.environ.get("TIDB_PORT", 4000)),
        user=os.environ["TIDB_USER"],
        password=os.environ["TIDB_PASSWORD"],
        database=os.environ["TIDB_DATABASE"],
        ssl={"ca": ssl_ca} if ssl_ca else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _decode(raw):
    """Decode a raw source_refs column value into a Python object (or None)."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", "ignore")
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return raw  # a bare scalar string
    return raw


def _analyze(decoded):
    """Diagnostics for a decoded source_refs value.

    Returns (before_count, nested_arrays, malformed_dropped). before_count is
    the number of top-level elements; nested_arrays counts array nodes below
    the root; malformed_dropped counts scalar leaves that fail the ref regex.
    """
    if decoded is None:
        return 0, 0, 0
    if isinstance(decoded, list):
        before_count = len(decoded)
    else:
        before_count = 1
    nested = 0
    malformed = 0

    def _walk(v, depth):
        nonlocal nested, malformed
        if isinstance(v, list):
            if depth > 0:
                nested += 1
            for item in v:
                _walk(item, depth + 1)
        elif isinstance(v, str):
            if not _SOURCE_REF_RE.match(v):
                malformed += 1
        else:
            malformed += 1

    _walk(decoded, 0)
    return before_count, nested, malformed


def phase1_flatten(conn, dry_run):
    """Flatten every fleet_memory.source_refs to a flat deduped scalar list."""
    print("\n=== PHASE 1: flatten source_refs ===")
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM fleet_memory ORDER BY id")
        ids = [r["id"] for r in cur.fetchall()]

    changed = 0
    for mem_id in ids:
        conn.begin()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT source_refs FROM fleet_memory WHERE id = %s "
                    "FOR UPDATE",
                    (mem_id,),
                )
                row = cur.fetchone()
                raw = row["source_refs"] if row else None
                decoded = _decode(raw)
                flat = _flatten_source_refs(decoded)
                before_count, nested, malformed = _analyze(decoded)

                # No-op if already flat-and-equal (idempotency). A NULL with
                # no refs is left NULL rather than rewritten to "[]".
                is_noop = (decoded is None and not flat) or (decoded == flat)
                if is_noop:
                    conn.rollback()
                    continue

                changed += 1
                print(
                    f"  id={mem_id}: refs {before_count} -> {len(flat)} "
                    f"(nested_flattened={nested}, malformed_dropped={malformed})"
                )
                if not dry_run:
                    cur.execute(
                        "UPDATE fleet_memory SET source_refs = %s WHERE id = %s",
                        (json.dumps(flat), mem_id),
                    )
                    conn.commit()
                else:
                    conn.rollback()
        except Exception:
            conn.rollback()
            raise
    print(f"  phase 1 {'would change' if dry_run else 'changed'} {changed} row(s).")


def phase2_backfill(conn, dry_run):
    """Recompute verified counters + corroborations/supersede_events + confidence."""
    print("\n=== PHASE 2: backfill counters + re-derive confidence ===")
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM fleet_memory ORDER BY id")
        ids = [r["id"] for r in cur.fetchall()]

    changed = 0
    for mem_id in ids:
        conn.begin()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT provenance, source_refs, confidence,
                           confirmations, contradictions,
                           verified_confirmations, verified_contradictions,
                           corroborations, supersede_events
                    FROM fleet_memory WHERE id = %s FOR UPDATE
                    """,
                    (mem_id,),
                )
                row = cur.fetchone()
                if not row:
                    conn.rollback()
                    continue

                provenance = row["provenance"]
                flat = _flatten_source_refs(_decode(row["source_refs"]))

                # DISTINCT linked reasoning ids (post-repair, refs may repeat
                # in history, so dedup is mandatory).
                reasoning_ids = set()
                for ref in flat:
                    m = _AGENT_REASONING_REF_RE.match(ref)
                    if m:
                        reasoning_ids.add(int(m.group(1)))

                v_conf = 0
                v_contra = 0
                if reasoning_ids:
                    placeholders = ",".join(["%s"] * len(reasoning_ids))
                    cur.execute(
                        f"""
                        SELECT verified_outcome
                        FROM agent_reasoning
                        WHERE id IN ({placeholders})
                        """,
                        list(reasoning_ids),
                    )
                    for ar in cur.fetchall():
                        outcome = ar["verified_outcome"]
                        if outcome == "fixed_as_diagnosed":
                            v_conf += 1
                        elif outcome in ("different_fault", "no_fault_found"):
                            v_contra += 1

                corroborations = int(row["confirmations"])   # OLD confirmations
                supersede_events = int(row["contradictions"])  # OLD contradictions
                new_conf = derive_confidence(provenance, v_conf, v_contra)

                old_conf = float(row["confidence"]) if row["confidence"] is not None else None
                unchanged = (
                    int(row["verified_confirmations"]) == v_conf
                    and int(row["verified_contradictions"]) == v_contra
                    and int(row["corroborations"]) == corroborations
                    and int(row["supersede_events"]) == supersede_events
                    and old_conf == new_conf
                )
                if unchanged:
                    conn.rollback()
                    continue

                changed += 1
                print(
                    f"  id={mem_id}: verified_confirmations={v_conf} "
                    f"verified_contradictions={v_contra} "
                    f"corroborations={corroborations} "
                    f"supersede_events={supersede_events} "
                    f"confidence {old_conf}->{new_conf} (prov={provenance})"
                )
                if not dry_run:
                    cur.execute(
                        """
                        UPDATE fleet_memory
                        SET verified_confirmations = %s,
                            verified_contradictions = %s,
                            corroborations = %s,
                            supersede_events = %s,
                            confidence = %s
                        WHERE id = %s
                        """,
                        (v_conf, v_contra, corroborations, supersede_events,
                         new_conf, mem_id),
                    )
                    conn.commit()
                else:
                    conn.rollback()
        except Exception:
            conn.rollback()
            raise
    print(f"  phase 2 {'would change' if dry_run else 'changed'} {changed} row(s).")


def main():
    parser = argparse.ArgumentParser(
        description="Flatten source_refs and backfill v4 split counters.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the full per-row summary without writing anything.",
    )
    args = parser.parse_args()

    mode = "DRY-RUN (no writes)" if args.dry_run else "APPLY (writing changes)"
    print(f"v4 source_refs repair + counter backfill — {mode}")

    conn = _connect()
    try:
        phase1_flatten(conn, args.dry_run)
        phase2_backfill(conn, args.dry_run)
    finally:
        conn.close()

    print("\nDone." + ("  (dry-run — nothing was written)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
