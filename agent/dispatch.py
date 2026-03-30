#!/usr/bin/env python3
"""
EV Charger Fleet Dispatcher — Concurrent Multi-Agent Investigations
====================================================================
Usage:
    # Investigate top 5 anomalous chargers concurrently (default)
    python3 agent/dispatch.py

    # Investigate top N chargers
    python3 agent/dispatch.py --top 10

    # Limit concurrency
    python3 agent/dispatch.py --top 10 --workers 3

    # Investigate specific chargers concurrently
    python3 agent/dispatch.py --chargers CP-IE-TEST-00042,CP-IE-TEST-00072

    # Dry run — show dispatch plan without running agents
    python3 agent/dispatch.py --top 10 --dry-run
"""

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from uuid import uuid4

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tool_handlers import run_agent, get_db  # noqa: E402

_WIDE = 43
_NARROW = 41


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def find_top_anomalous_chargers(n: int) -> list[dict]:
    """Return top-N distinct chargers by highest anomaly_score in last 24h."""
    db = get_db()
    try:
        with db.cursor() as cur:
            # Fetch more rows than N to allow dedup by charger_id
            cur.execute("""
                SELECT charger_id, window_start, anomaly_score, anomaly_flags,
                       avg_power_w, voltage_stddev, max_temp_c, error_count,
                       max_earth_leak, avg_fan_rpm
                FROM charger_windows
                WHERE anomaly_score > 0
                  AND window_start > NOW() - INTERVAL 24 HOUR
                ORDER BY anomaly_score DESC
                LIMIT %s
            """, (n * 10,))
            rows = cur.fetchall()
    finally:
        db.close()

    # Dedup: keep only the highest-scoring window per charger_id
    seen: set[str] = set()
    unique: list[dict] = []
    for row in rows:
        cid = row["charger_id"]
        if cid not in seen:
            seen.add(cid)
            unique.append(row)
        if len(unique) >= n:
            break

    return unique


def find_window_for_charger(charger_id: str) -> dict | None:
    """Return the most recent anomalous window for a specific charger."""
    db = get_db()
    try:
        with db.cursor() as cur:
            cur.execute("""
                SELECT charger_id, window_start, anomaly_score, anomaly_flags,
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


# ---------------------------------------------------------------------------
# Trigger builder (mirrors run_agent.py)
# ---------------------------------------------------------------------------

def build_trigger_from_window(charger_id: str, window: dict) -> str:
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
# Display helpers
# ---------------------------------------------------------------------------

def _anomaly_detail(window: dict) -> str:
    """Single most prominent anomaly metric for the dispatch plan table."""
    if window.get("max_earth_leak") and float(window["max_earth_leak"]) > 0:
        return f"earth_leak={window['max_earth_leak']}mA"
    if window.get("voltage_stddev") and float(window["voltage_stddev"]) > 0:
        return f"voltage_stddev={window['voltage_stddev']}V"
    if window.get("error_count") and int(window["error_count"]) > 0:
        return f"error_count={window['error_count']}"
    if window.get("max_temp_c") and float(window["max_temp_c"]) > 0:
        return f"temp={window['max_temp_c']}C"
    if window.get("avg_fan_rpm") is not None:
        return f"fan_rpm={window['avg_fan_rpm']}"
    return "anomaly_score_only"


def print_dispatch_plan(targets: list[dict], dispatch_id: str, workers: int):
    n = len(targets)
    print(f"{'═' * _WIDE}", file=sys.stderr)
    print(f"Fleet Dispatch — {n} concurrent investigation{'s' if n != 1 else ''}", file=sys.stderr)
    print(f"Dispatch ID: disp-{dispatch_id}", file=sys.stderr)
    print(f"Workers:     {workers}", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)
    print("", file=sys.stderr)
    for t in targets:
        detail = _anomaly_detail(t["window"])
        print(
            f"  {t['charger_id']:<24}  anomaly_score={t['window']['anomaly_score']:<8}  {detail}",
            file=sys.stderr,
        )
    print("", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)


def print_agent_report(index: int, total: int, charger_id: str, result: dict | str):
    response = result["response"] if isinstance(result, dict) else result
    print(f"\n{'━' * _NARROW}")
    print(f"[{index}/{total}] {charger_id} — Investigation Report")
    print(f"{'━' * _NARROW}")
    print(response)


def print_fleet_summary(
    total: int,
    total_tool_calls: int,
    checkpoints: int,
    fleet_writes: int,
    wall_time: float,
    durations: list[float],
):
    avg_dur = sum(durations) / len(durations) if durations else 0.0
    min_dur = min(durations) if durations else 0.0
    max_dur = max(durations) if durations else 0.0
    speedup = sum(durations) / wall_time if wall_time > 0 else 0.0

    print(f"\n{'═' * _WIDE}", file=sys.stderr)
    print(f"Fleet Dispatch Summary", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)
    print(f"  Chargers investigated:  {total}", file=sys.stderr)
    print(f"  Total tool calls:       {total_tool_calls}", file=sys.stderr)
    print(f"  Reasoning checkpoints:  {checkpoints} written", file=sys.stderr)
    print(f"  Fleet memories:         {fleet_writes} written", file=sys.stderr)
    print(f"  Wall-clock time:        {wall_time:.1f}s", file=sys.stderr)
    print(
        f"  Agent durations:        avg {avg_dur:.1f}s, "
        f"min {min_dur:.1f}s, max {max_dur:.1f}s",
        file=sys.stderr,
    )
    print(f"  Concurrency speedup:    ~{speedup:.1f}x vs sequential", file=sys.stderr)
    print(f"{'═' * _WIDE}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Agent worker
# ---------------------------------------------------------------------------

def _run_one(
    index: int,
    total: int,
    session_id: str,
    charger_id: str,
    trigger: str,
) -> dict:
    """Run a single agent investigation. Returns result augmented with timing."""
    t0 = time.monotonic()
    result = run_agent(
        session_id=session_id,
        user_id="dispatcher",
        charger_id=charger_id,
        trigger=trigger,
    )
    duration = time.monotonic() - t0

    # Normalise: handle legacy plain-string return (spec says handle both)
    if isinstance(result, str):
        result = {
            "response": result,
            "tool_calls": {},
            "context_sources": [],
            "context_tokens": 0,
        }

    result["_charger_id"] = charger_id
    result["_session_id"] = session_id
    result["_index"] = index
    result["_duration"] = duration
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="EV Charger Fleet Dispatcher — concurrent multi-agent investigations"
    )
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument(
        "--top", type=int, default=5, metavar="N",
        help="Investigate the top-N most anomalous chargers (default: 5)",
    )
    target_group.add_argument(
        "--chargers", metavar="ID,ID,...",
        help="Comma-separated list of specific charger IDs to investigate",
    )
    parser.add_argument(
        "--workers", type=int, default=None, metavar="N",
        help="Max concurrent workers (default: min(N, 5))",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Show the dispatch plan without running agents",
    )
    args = parser.parse_args()

    dispatch_id = uuid4().hex[:8]

    # ------------------------------------------------------------------
    # Resolve targets
    # ------------------------------------------------------------------
    targets: list[dict] = []  # [{charger_id, window, trigger, session_id}]

    if args.chargers:
        charger_ids = [c.strip() for c in args.chargers.split(",") if c.strip()]
        for cid in charger_ids:
            window = find_window_for_charger(cid)
            if window is None:
                print(f"Warning: no anomalous window found for {cid}, skipping.", file=sys.stderr)
                continue
            targets.append({"charger_id": cid, "window": window})
    else:
        windows = find_top_anomalous_chargers(args.top)
        if not windows:
            print("No anomalous charger windows found in the last 24 hours.", file=sys.stderr)
            sys.exit(1)
        for w in windows:
            targets.append({"charger_id": w["charger_id"], "window": w})

    if not targets:
        print("No targets to investigate.", file=sys.stderr)
        sys.exit(1)

    # Assign session IDs and build triggers
    for i, t in enumerate(targets):
        t["session_id"] = f"dispatch-{dispatch_id}-{i}"
        t["trigger"] = build_trigger_from_window(t["charger_id"], t["window"])

    n = len(targets)
    workers = args.workers if args.workers is not None else min(n, 5)

    # ------------------------------------------------------------------
    # Dispatch plan
    # ------------------------------------------------------------------
    print_dispatch_plan(targets, dispatch_id, workers)

    if args.dry_run:
        print("Dry run — no agents started.", file=sys.stderr)
        return

    # ------------------------------------------------------------------
    # Concurrent execution
    # ------------------------------------------------------------------
    # ordered_results[index] = result dict, filled as futures complete
    ordered_results: dict[int, dict] = {}
    wall_start = time.monotonic()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_index = {}
        for i, t in enumerate(targets):
            future = executor.submit(
                _run_one,
                i + 1,          # 1-based display index
                n,
                t["session_id"],
                t["charger_id"],
                t["trigger"],
            )
            future_to_index[future] = i

        for future in as_completed(future_to_index):
            i = future_to_index[future]
            t = targets[i]
            try:
                result = future.result()
                ordered_results[i] = result
                # Progress line to stderr
                response_preview = result["response"][:80].replace("\n", " ")
                print(
                    f"Agent {result['_index']}/{n} complete: "
                    f"{t['charger_id']} — {response_preview}...",
                    file=sys.stderr,
                )
            except Exception as exc:
                print(
                    f"Agent {i + 1}/{n} FAILED: {t['charger_id']} — {exc}",
                    file=sys.stderr,
                )
                ordered_results[i] = {
                    "response": f"[Agent failed: {exc}]",
                    "tool_calls": {},
                    "context_sources": [],
                    "context_tokens": 0,
                    "_charger_id": t["charger_id"],
                    "_session_id": t["session_id"],
                    "_index": i + 1,
                    "_duration": 0.0,
                }

    wall_time = time.monotonic() - wall_start

    # ------------------------------------------------------------------
    # Print reports in original order
    # ------------------------------------------------------------------
    total_tool_calls = 0
    total_checkpoints = 0
    total_fleet_writes = 0
    durations: list[float] = []

    for i in range(n):
        result = ordered_results[i]
        print_agent_report(i + 1, n, result["_charger_id"], result)

        tc = result.get("tool_calls", {})
        total_tool_calls += sum(tc.values())
        total_checkpoints += tc.get("write_reasoning_checkpoint", 0)
        total_fleet_writes += tc.get("write_fleet_memory", 0)
        dur = result.get("_duration", 0.0)
        if dur > 0:
            durations.append(dur)

    # ------------------------------------------------------------------
    # Fleet summary
    # ------------------------------------------------------------------
    print_fleet_summary(
        total=n,
        total_tool_calls=total_tool_calls,
        checkpoints=total_checkpoints,
        fleet_writes=total_fleet_writes,
        wall_time=wall_time,
        durations=durations,
    )


if __name__ == "__main__":
    main()
