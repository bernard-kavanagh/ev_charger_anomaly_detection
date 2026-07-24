#!/usr/bin/env python3
"""
v4 data repair — flatten source_refs, then backfill the split counters
======================================================================
Two-phase, ordered-by-construction repair for the v4 counter split. Run this
AFTER applying migrations/schema_v4_counter_split.sql (which adds the
verified_confirmations / verified_contradictions / corroborations /
supersede_events columns) and BEFORE relying on the new derived confidence.

  Phase 0 — ref-grammar discovery (dry-run diagnostic, no writes ever)
      Parses (does not regex-guess) every ref across fleet_memory.source_refs
      and agent_reasoning.evidence_refs, recovering stringified layers, and
      prints the DISTINCT prefixes/formats actually present plus any value the
      widened validation pattern still rejects. This is how the true grammar
      is established empirically before Phase 1 relies on it.

  Phase 1 — source_refs flattening
      Two pre-v4 corruptions buried refs below the top level:
        * stringified arrays — the merge path appended json.dumps(list) as a
          single *string* element, e.g. [..., '["agent_reasoning:X"]']. This
          is what the cluster-2 dry-run actually found (nested_flattened=0,
          malformed_dropped up to 20/row under the old regex-only classifier).
        * nested arrays — [..., ["agent_reasoning:X"]].
      Both are invisible to verify_outcome's JSON_CONTAINS on a top-level
      scalar. Phase 1 recursively flattens every fleet_memory.source_refs into
      a deduped flat list of scalar strings matching ^[a-z_]+:[A-Za-z0-9._-]+$,
      RECOVERING stringified layers (parse + recurse to fixpoint) rather than
      dropping them. An element is counted malformed_dropped only if it is both
      unparseable as JSON AND fails the ref pattern. Each row is repaired
      inside its own transaction with SELECT ... FOR UPDATE.

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
    _flatten_source_refs_with_stats,
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


def _before_count(decoded):
    """Number of top-level elements in a decoded source_refs value."""
    if decoded is None:
        return 0
    if isinstance(decoded, list):
        return len(decoded)
    return 1


def phase0_discover(conn):
    """Empirically establish the ref grammar (parse, don't regex-guess).

    Scans every ref across fleet_memory.source_refs and
    agent_reasoning.evidence_refs, recovering stringified layers the same way
    Phase 1 will, then reports the DISTINCT prefixes/formats present and any
    value the widened pattern still rejects. Read-only: never writes, runs in
    both dry-run and apply modes so the grammar is on the record either way.
    """
    print("\n=== PHASE 0: ref-grammar discovery (read-only) ===")

    sources = (
        ("fleet_memory", "source_refs"),
        ("agent_reasoning", "evidence_refs"),
    )
    prefix_counts = {}          # prefix -> count of accepted refs
    prefix_samples = {}         # prefix -> a sample accepted ref
    rejected = {}               # raw rejected value -> count

    for table, column in sources:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {column} AS refs FROM {table}")
            rows = cur.fetchall()
        for row in rows:
            flat, stats = _flatten_source_refs_with_stats(_decode(row["refs"]))
            for ref in flat:
                prefix = ref.split(":", 1)[0]
                prefix_counts[prefix] = prefix_counts.get(prefix, 0) + 1
                prefix_samples.setdefault(prefix, ref)
            for bad in stats["unmatched_formats"]:
                rejected[bad] = rejected.get(bad, 0) + 1

    print(f"  validation pattern: {_SOURCE_REF_RE.pattern}")
    if prefix_counts:
        print("  distinct ref prefixes accepted by the widened pattern:")
        for prefix in sorted(prefix_counts):
            print(
                f"    {prefix:<24} n={prefix_counts[prefix]:<6} "
                f"e.g. {prefix_samples[prefix]}"
            )
    else:
        print("  (no accepted refs found)")

    if rejected:
        print(
            "  !! ref-shaped values REJECTED by the widened pattern "
            "(NOT dropped silently — widen the grammar if these are real):"
        )
        for bad in sorted(rejected, key=lambda k: (-rejected[k], k)):
            print(f"    n={rejected[bad]:<6} {bad!r}")
    else:
        print("  no ref-shaped values were rejected by the widened pattern.")


def phase1_flatten(conn, dry_run):
    """Flatten every fleet_memory.source_refs to a flat deduped scalar list,
    recovering stringified layers rather than dropping them."""
    print("\n=== PHASE 1: flatten source_refs ===")
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM fleet_memory ORDER BY id")
        ids = [r["id"] for r in cur.fetchall()]

    changed = 0
    tot_recovered = tot_nested = tot_dropped = 0
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
                flat, stats = _flatten_source_refs_with_stats(decoded)
                before_count = _before_count(decoded)

                # No-op if already flat-and-equal (idempotency). A NULL with
                # no refs is left NULL rather than rewritten to "[]".
                is_noop = (decoded is None and not flat) or (decoded == flat)
                if is_noop:
                    conn.rollback()
                    continue

                changed += 1
                tot_recovered += stats["recovered_from_strings"]
                tot_nested += stats["nested_flattened"]
                tot_dropped += stats["malformed_dropped"]
                print(
                    f"  id={mem_id}: refs {before_count} -> {len(flat)} "
                    f"(recovered_from_strings={stats['recovered_from_strings']}, "
                    f"nested_flattened={stats['nested_flattened']}, "
                    f"malformed_dropped={stats['malformed_dropped']})"
                )
                if stats["unmatched_formats"]:
                    print(
                        f"      rejected (not dropped silently): "
                        f"{stats['unmatched_formats']}"
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
    print(
        f"  phase 1 {'would change' if dry_run else 'changed'} {changed} row(s) "
        f"— totals: recovered_from_strings={tot_recovered}, "
        f"nested_flattened={tot_nested}, malformed_dropped={tot_dropped}."
    )


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
        phase0_discover(conn)
        phase1_flatten(conn, args.dry_run)
        # Phase 2 MUST run after the improved Phase 1: recovered refs can
        # change the set of linked checkpoints, hence the verified counters.
        phase2_backfill(conn, args.dry_run)
    finally:
        conn.close()

    print("\nDone." + ("  (dry-run — nothing was written)" if args.dry_run else ""))


if __name__ == "__main__":
    main()
