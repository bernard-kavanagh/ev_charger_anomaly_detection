#!/usr/bin/env python3
"""
Field-tech outcome write-back — CLI
===================================
Records the VERIFIED real-world outcome of a diagnosis and lets the
platform re-derive confidence from ground truth.

This is intentionally a separate entry point from the agent: verify_outcome
is NOT one of the agent's tools. Only a human (field tech / reviewer) who
observed the actual repair should call this, otherwise the model would be
grading its own homework.

Usage:
    python agent/verify_outcome.py --reasoning-id 48291 --outcome fixed_as_diagnosed
    python agent/verify_outcome.py --reasoning-id 48291 --outcome different_fault \\
        --notes "Turned out to be a loose neutral, not the contactor."

Legacy linkage (v5): the two pre-v5 orphaned checkpoints had their refs
stamped into no memory (fleet_memory has no session column, so the link is
not derivable). Re-link them BY HAND before propagating:

    python agent/verify_outcome.py --reasoning-id 8070450532248918833 \\
        --outcome fixed_as_diagnosed --link-memory 8070450532248228834
"""

import argparse
import json
import sys
from pathlib import Path

# Load .env before importing tool_handlers so all env vars are present.
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Add project root to sys.path so `import tool_handlers` works regardless of CWD.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tool_handlers import (  # noqa: E402  (after sys.path patch)
    verify_outcome, link_checkpoint_to_memory,
)


def _banner(line: str) -> None:
    """Print a loud, un-missable banner to stderr."""
    bar = "!" * 72
    print(bar, file=sys.stderr)
    print(line, file=sys.stderr)
    print(bar, file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Record a field-verified diagnosis outcome.",
    )
    parser.add_argument(
        "--reasoning-id", type=int, required=True,
        help="agent_reasoning.id of the diagnosis being verified.",
    )
    parser.add_argument(
        "--outcome", required=True,
        choices=["fixed_as_diagnosed", "different_fault", "no_fault_found"],
        help="The verified real-world outcome.",
    )
    parser.add_argument(
        "--notes", default=None,
        help="Optional free-text notes from the field.",
    )
    parser.add_argument(
        "--link-memory", type=int, default=None, metavar="MEMORY_ID",
        help="Legacy v5 linkage: BEFORE propagating, manually stamp this "
             "checkpoint's ref into fleet_memory[MEMORY_ID].pending_refs. Use "
             "only for the pre-v5 orphaned checkpoints whose refs were never "
             "stamped at write time. The link is human-ASSERTED, not derived.",
    )
    args = parser.parse_args()

    # Legacy linkage FIRST, so the subsequent verify_outcome propagation can
    # find the memory via pending_refs.
    if args.link_memory is not None:
        link_result = json.loads(link_checkpoint_to_memory(
            reasoning_id=args.reasoning_id,
            memory_id=args.link_memory,
        ))
        if link_result.get("error"):
            _banner(f"MANUAL LINK FAILED: {link_result['error']}")
            print(json.dumps(link_result, indent=2, default=str))
            sys.exit(2)
        _banner(
            "MANUAL LINK ASSERTED (human, not derived): "
            f"agent_reasoning:{args.reasoning_id} -> fleet_memory "
            f"{args.link_memory} pending_refs"
            + (" [already linked, no-op]"
               if link_result.get("already_linked") else "")
        )
        print(json.dumps(link_result, indent=2, default=str))

    result = json.loads(verify_outcome(
        reasoning_id=args.reasoning_id,
        outcome=args.outcome,
        notes=args.notes,
    ))

    status = result.get("status")
    if status == "ok_no_propagation":
        # Loud failure: the outcome was recorded but reached no memory. This is
        # the severed-loop symptom — never let it pass as a quiet success.
        _banner("WARNING — verify_outcome: NO PROPAGATION\n"
                + str(result.get("warning", "")))
    elif status == "outcome_conflict":
        _banner(
            "OUTCOME CONFLICT — nothing changed. "
            f"existing={result.get('existing_outcome')!r} "
            f"incoming={result.get('incoming_outcome')!r} "
            f"verified_at={result.get('verified_at')}"
        )
    elif status == "ok":
        n = len(result.get("touched_memories", []))
        print(f"OK — propagated to {n} fleet memory(ies).", file=sys.stderr)

    print(json.dumps(result, indent=2, default=str))

    # Non-zero exit on non-propagation / conflict so scripted callers notice.
    if status in ("ok_no_propagation", "outcome_conflict"):
        sys.exit(3)


if __name__ == "__main__":
    main()
