#!/usr/bin/env python3
"""
EV Charger Fleet Diagnostic Agent — CLI Runner
===============================================
Usage:
    # Investigate a specific charger with an explicit trigger
    python agent/run_agent.py --charger CP-IE-TEST-00042 --trigger "Anomaly score 0.73..."

    # Auto-build trigger from the charger's most recent anomalous window
    python agent/run_agent.py --charger CP-IE-TEST-00042

    # Find the highest-scoring charger in the last 24h and investigate it
    python agent/run_agent.py --auto
"""

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4

# Load .env before importing tool_handlers so all env vars are present.
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Add project root to sys.path so `import tool_handlers` works regardless of CWD.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import tool_handlers  # noqa: E402  (after sys.path patch)
from tool_handlers import run_agent, get_db, embed  # noqa: E402


# ---------------------------------------------------------------------------
# Outage catalog bootstrap
# ---------------------------------------------------------------------------

def _build_outage_text(row: dict) -> str:
    """Matches the text builder in embedding_service.py for outage_catalog."""
    symptoms = row.get("symptoms", "")
    if isinstance(symptoms, (list, dict)):
        symptoms = json.dumps(symptoms)
    parts = [
        f"Pattern: {row.get('pattern_name', '')}",
        f"Category: {row.get('category', '')}",
        f"Severity: {row.get('severity', '')}",
        f"Root cause: {row.get('root_cause', '')}",
        f"Symptoms: {symptoms}",
        f"Resolution: {row.get('resolution', '')}",
    ]
    return ". ".join(parts)


def bootstrap_outage_catalog():
    """Embed any outage_catalog rows that have NULL signature_vec."""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute(
                "SELECT id, pattern_name, category, severity, "
                "root_cause, symptoms, resolution "
                "FROM outage_catalog WHERE signature_vec IS NULL"
            )
            rows = cur.fetchall()
    finally:
        db.close()

    if not rows:
        return

    print(f"Bootstrap: embedding {len(rows)} outage_catalog row(s) with NULL signature_vec...",
          file=sys.stderr)

    pairs = []
    for row in rows:
        vec = embed(_build_outage_text(row))
        pairs.append((row["id"], vec))

    db = get_db()
    try:
        with db.cursor() as cur:
            for row_id, vec in pairs:
                cur.execute(
                    "UPDATE outage_catalog SET signature_vec = %s WHERE id = %s",
                    (str(vec), row_id)
                )
    finally:
        db.close()

    print(f"Bootstrap: done ({len(rows)} row(s) embedded).", file=sys.stderr)


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------

def find_most_anomalous_window() -> dict | None:
    """Return the highest-scoring charger_windows row in the last 24 hours."""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT charger_id, window_start, anomaly_score, anomaly_flags,
                       avg_power_w, voltage_stddev, max_temp_c, error_count,
                       max_earth_leak, avg_fan_rpm
                FROM charger_windows
                WHERE anomaly_score > 0
                  AND window_start > NOW() - INTERVAL 24 HOUR
                ORDER BY anomaly_score DESC
                LIMIT 1
            """)
            return cur.fetchone()
    finally:
        db.close()


def find_recent_anomalous_window(charger_id: str) -> dict | None:
    """Return the most recent anomalous window for a specific charger."""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT window_start, anomaly_score, anomaly_flags,
                       avg_power_w, voltage_stddev, max_temp_c, error_count,
                       max_earth_leak, avg_fan_rpm
                FROM charger_windows
                WHERE charger_id = %s AND anomaly_score > 0
                ORDER BY window_start DESC
                LIMIT 1
            """, (charger_id,))
            return cur.fetchone()
    finally:
        db.close()


def build_trigger_from_window(charger_id: str, window: dict) -> str:
    """Build a natural-language trigger string from a charger_windows row."""
    parts = [f"Anomaly score {window['anomaly_score']} detected for charger {charger_id}"]

    if window.get("voltage_stddev") and float(window["voltage_stddev"]) > 0:
        parts.append(f"voltage stddev {window['voltage_stddev']}V")
    if window.get("max_temp_c") and float(window["max_temp_c"]) > 0:
        parts.append(f"max temp {window['max_temp_c']}°C")
    if window.get("avg_power_w") and float(window["avg_power_w"]) > 0:
        parts.append(f"avg power {window['avg_power_w']}W")
    if window.get("error_count") and int(window["error_count"]) > 0:
        parts.append(f"error count {window['error_count']}")
    if window.get("max_earth_leak") and float(window["max_earth_leak"]) > 0:
        parts.append(f"earth leakage {window['max_earth_leak']}mA")
    if window.get("avg_fan_rpm") is not None:
        parts.append(f"avg fan RPM {window['avg_fan_rpm']}")

    flags = window.get("anomaly_flags")
    if flags:
        if isinstance(flags, str):
            try:
                flags = json.loads(flags)
            except (json.JSONDecodeError, ValueError):
                pass
        if isinstance(flags, list) and flags:
            parts.append(f"flags: {', '.join(flags)}")

    parts.append(f"window start {window['window_start']}")
    return ". ".join(parts) + "."


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_WIDE = 43

def print_header(session_id: str, charger_id: str, trigger: str):
    print(f"{'═' * _WIDE}", file=sys.stderr)
    print(f" EV Charger Fleet Diagnostic Agent", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)
    print(f" Session:  {session_id}", file=sys.stderr)
    print(f" Charger:  {charger_id}", file=sys.stderr)
    print(f" Trigger:  {trigger[:80]}{'...' if len(trigger) > 80 else ''}", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)


def print_footer(result: dict, session_id: str):
    tool_summary = ", ".join(
        f"{name} ({count})"
        for name, count in sorted(result["tool_calls"].items())
    ) or "none"
    print(f"\n{'─' * _WIDE}", file=sys.stderr)
    print(f" Agent complete", file=sys.stderr)
    print(f" Context:    {len(result['context_sources'])} sources, "
          f"{result['context_tokens']} tokens", file=sys.stderr)
    print(f" Tool calls: {tool_summary}", file=sys.stderr)
    print(f" Session:    {session_id}", file=sys.stderr)
    print(f"{'─' * _WIDE}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="EV Charger Fleet Diagnostic Agent CLI"
    )
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--charger", metavar="CHARGER_ID",
                        help="Charger ID to investigate")
    target.add_argument("--auto", action="store_true",
                        help="Auto-select the charger with the highest recent anomaly score")
    parser.add_argument("--trigger", metavar="TEXT",
                        help="Explicit trigger text (only with --charger)")
    args = parser.parse_args()

    if args.trigger and args.auto:
        parser.error("--trigger cannot be used with --auto")

    # Bootstrap: ensure outage_catalog has embeddings
    bootstrap_outage_catalog()

    # Resolve charger_id and trigger
    if args.auto:
        window = find_most_anomalous_window()
        if not window:
            print("No anomalous windows found in the last 24 hours.", file=sys.stderr)
            sys.exit(1)
        charger_id = window["charger_id"]
        trigger = build_trigger_from_window(charger_id, window)
        print(f"Auto-selected charger: {charger_id} "
              f"(anomaly score {window['anomaly_score']})", file=sys.stderr)
    else:
        charger_id = args.charger
        if args.trigger:
            trigger = args.trigger
        else:
            window = find_recent_anomalous_window(charger_id)
            if not window:
                print(f"No anomalous windows found for {charger_id}.", file=sys.stderr)
                sys.exit(1)
            trigger = build_trigger_from_window(charger_id, window)
            print(f"Using most recent anomalous window for {charger_id} "
                  f"(score {window['anomaly_score']})", file=sys.stderr)

    session_id = f"sess-{uuid4().hex[:8]}"
    print_header(session_id, charger_id, trigger)

    result = run_agent(
        session_id=session_id,
        user_id="cli",
        charger_id=charger_id,
        trigger=trigger,
    )

    print_footer(result, session_id)
    print(result["response"])


if __name__ == "__main__":
    main()
