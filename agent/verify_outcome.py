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
"""

import argparse
import sys
from pathlib import Path

# Load .env before importing tool_handlers so all env vars are present.
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

# Add project root to sys.path so `import tool_handlers` works regardless of CWD.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from tool_handlers import verify_outcome  # noqa: E402  (after sys.path patch)


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
    args = parser.parse_args()

    result = verify_outcome(
        reasoning_id=args.reasoning_id,
        outcome=args.outcome,
        notes=args.notes,
    )
    print(result)


if __name__ == "__main__":
    main()
