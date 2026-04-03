"""
Observability Module
=====================
Structured JSON logging for the EV charger diagnostic agent.
Emits one log line per event with consistent schema for downstream
ingestion (CloudWatch, Datadog, stdout → jq, etc.).

Usage:
    from observability import AgentObserver

    obs = AgentObserver(session_id="sess-001", charger_id="CP-IE-TEST-00042")
    obs.tool_call_start("search_similar_outages", {"window_id": 42})
    result = handler(...)
    obs.tool_call_end("search_similar_outages", result_size=5)
    obs.context_assembled(tokens_used=1200, sources=["profile", "anomalies"])
    obs.agent_complete(tool_calls={"search_similar_outages": 2}, response_tokens=450)

    # Get summary for dispatch.py fleet report
    summary = obs.summary()
"""

import json
import time
import logging
from datetime import datetime, timezone
from typing import Optional


log = logging.getLogger("agent-obs")


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON line."""

    def format(self, record):
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if hasattr(record, "event_data"):
            entry.update(record.event_data)
        return json.dumps(entry, default=str)


def configure_logging(level: str = "INFO"):
    """Configure structured JSON logging on the agent-obs logger."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    obs_log = logging.getLogger("agent-obs")
    obs_log.handlers = [handler]
    obs_log.setLevel(getattr(logging, level.upper(), logging.INFO))
    obs_log.propagate = False


class AgentObserver:
    """
    Collects structured telemetry for a single agent invocation.

    Tracks:
    - Tool call latencies and result sizes
    - Context assembly time and token budget usage
    - Vector search distances (quality signal)
    - Agent loop iterations and circuit breaker triggers
    - Total wall-clock time
    """

    def __init__(self, session_id: str, charger_id: str):
        self.session_id = session_id
        self.charger_id = charger_id
        self.start_time = time.monotonic()
        self.events: list[dict] = []
        self._tool_timers: dict[str, float] = {}

        # Aggregates
        self.tool_calls: dict[str, int] = {}
        self.tool_latencies: dict[str, list[float]] = {}
        self.vector_distances: list[float] = []
        self.context_tokens_used: int = 0
        self.context_sources: list[str] = []
        self.loop_iterations: int = 0
        self.circuit_breaker_triggered: bool = False

    def _emit(self, event_type: str, data: dict):
        """Emit a structured log event."""
        entry = {
            "event": event_type,
            "session_id": self.session_id,
            "charger_id": self.charger_id,
            "elapsed_ms": round((time.monotonic() - self.start_time) * 1000),
            **data,
        }
        self.events.append(entry)

        record = log.makeRecord(
            "agent-obs", logging.INFO, "", 0, event_type, (), None
        )
        record.event_data = entry
        log.handle(record)

    # --- Tool calls ---

    def tool_call_start(self, tool_name: str, tool_input: dict):
        """Record the start of a tool call."""
        self._tool_timers[tool_name] = time.monotonic()
        self._emit("tool_call_start", {
            "tool": tool_name,
            "input_keys": list(tool_input.keys()),
        })

    def tool_call_end(self, tool_name: str, result_size: int = 0,
                      top_distance: Optional[float] = None,
                      error: Optional[str] = None):
        """Record the end of a tool call with latency."""
        start = self._tool_timers.pop(tool_name, None)
        latency_ms = round((time.monotonic() - start) * 1000) if start else 0

        self.tool_calls[tool_name] = self.tool_calls.get(tool_name, 0) + 1
        self.tool_latencies.setdefault(tool_name, []).append(latency_ms)

        if top_distance is not None:
            self.vector_distances.append(top_distance)

        data = {
            "tool": tool_name,
            "latency_ms": latency_ms,
            "result_size": result_size,
        }
        if top_distance is not None:
            data["top_distance"] = round(top_distance, 4)
        if error:
            data["error"] = error

        self._emit("tool_call_end", data)

    # --- Context assembly ---

    def context_assembled(self, tokens_used: int, token_budget: int,
                          sources: list[str], assembly_ms: int = 0):
        """Record context assembly result."""
        self.context_tokens_used = tokens_used
        self.context_sources = sources
        self._emit("context_assembled", {
            "tokens_used": tokens_used,
            "token_budget": token_budget,
            "budget_utilisation": round(tokens_used / token_budget, 2),
            "source_count": len(sources),
            "sources": sources,
            "assembly_ms": assembly_ms,
        })

    # --- Agent loop ---

    def loop_iteration(self, iteration: int, tool_calls_this_round: list[str]):
        """Record an agent loop iteration."""
        self.loop_iterations = iteration
        self._emit("loop_iteration", {
            "iteration": iteration,
            "tools_called": tool_calls_this_round,
        })

    def circuit_breaker(self, reason: str, tool_name: str):
        """Record a circuit breaker trigger."""
        self.circuit_breaker_triggered = True
        self._emit("circuit_breaker", {
            "reason": reason,
            "tool": tool_name,
        })

    # --- Completion ---

    def agent_complete(self, response_tokens: int = 0):
        """Record agent completion with summary metrics."""
        wall_ms = round((time.monotonic() - self.start_time) * 1000)

        avg_latencies = {}
        for tool, lats in self.tool_latencies.items():
            avg_latencies[tool] = round(sum(lats) / len(lats))

        self._emit("agent_complete", {
            "wall_ms": wall_ms,
            "loop_iterations": self.loop_iterations,
            "total_tool_calls": sum(self.tool_calls.values()),
            "tool_call_counts": self.tool_calls,
            "avg_tool_latencies_ms": avg_latencies,
            "context_tokens": self.context_tokens_used,
            "response_tokens": response_tokens,
            "circuit_breaker_triggered": self.circuit_breaker_triggered,
            "avg_vector_distance": (
                round(sum(self.vector_distances) / len(self.vector_distances), 4)
                if self.vector_distances else None
            ),
        })

    # --- Summary for dispatch reports ---

    def summary(self) -> dict:
        """Return a summary dict for fleet-level reporting."""
        wall_ms = round((time.monotonic() - self.start_time) * 1000)
        return {
            "session_id": self.session_id,
            "charger_id": self.charger_id,
            "wall_ms": wall_ms,
            "loop_iterations": self.loop_iterations,
            "tool_calls": dict(self.tool_calls),
            "total_tool_calls": sum(self.tool_calls.values()),
            "context_tokens": self.context_tokens_used,
            "context_sources": self.context_sources,
            "circuit_breaker_triggered": self.circuit_breaker_triggered,
        }
