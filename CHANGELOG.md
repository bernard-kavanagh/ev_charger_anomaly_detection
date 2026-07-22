# Changelog

Version history for the EV Charger IoT Platform cognitive foundation. Most recent first.

---

## v4 — 2026-07-22 — Derived-confidence refactor

This release kills the model-self-report anti-pattern across search and routing, and consolidates the v3 schema migration into `schema.sql`. Four commits: `1e674c1` and `be75145` rebuild hybrid search on TiDB `FTS_MATCH_WORD` as a *filter-and-rank* query — the keyword predicate appears bare in both `WHERE` and `SELECT` against a single full-text column per call, vector cosine distance is the sole ranker, and a SQL error (not an empty result) is what trips the vector-only `FULLTEXT_FALLBACK`. `40b1be1` adds the schema-v3 columns (`model_confidence`, `provenance`, `confirmations`/`contradictions`, `last_decayed_at`, `verified_outcome`, `verified_at`). `830d054` makes decision-grade confidence a *derived* Beta-posterior over observed corroboration (`derive_confidence()`), replaces the confidence-only shortcut gate with a structural one (confidence ≥ 0.85 **AND** (confirmations ≥ 3 **OR** provenance = 'verified') **AND** `superseded_by IS NULL` **AND** similarity ≥ 0.55), and adds the `verify_outcome` write-back path that closes the ground-truth loop from field techs into `fleet_memory`. The seven v3 migration columns are now merged into the canonical `schema.sql` CREATE TABLE blocks; `migrations/schema_v3_derived_confidence.sql` is retained as a historical record for existing v2 installs.

- **Model self-report is quarantined.** The `confidence` column now holds a platform-derived value; the model's raw number lives in a telemetry-only `model_confidence` column that no decision path reads.
- **`verify_outcome` is deliberately not an agent tool** (10th handler, not in the 9-tool list) — field-tech-driven only, so the model cannot verify its own diagnoses.
- **Test suite grew** to 76 unit tests across 13 classes plus 3 live-TiDB integration tests (gated on `RUN_INTEGRATION=1`).
- **Empirical result:** the same charger investigated before and after the refactor dropped from **8 tool calls to 4**, routing **EXPLORE → SHORTCUT** under the new structural gate — the corroborated pattern now earns the cheap path on evidence rather than a self-reported number. (Broader §4 numbers in [AGENT_LIFECYCLE.md](AGENT_LIFECYCLE.md) predate the refactor; re-measurement is pending.)

---

## v3 — 2026-05 — Routing layer

Introduced the external routing layer (Haiku shortcut path vs Sonnet explore path), Tier 5 graceful degradation, hard token caps on seed assembly, the slim summary call, system prompt caching, and charger-registry write-on-create. See [AGENT_LIFECYCLE.md](AGENT_LIFECYCLE.md) for the full agent flow, the multi-cluster empirical validation, and the reframed cost/quality thesis.

---

## v2 — 2026-04 — Audit upgrade

A 25-point architecture audit (22 green, 3 orange, 0 red) introduced hybrid search (vector + FULLTEXT), contradiction resolution via `superseded_by`, fleet-memory compaction, confidence decay, anomaly explainability, data validation, structured observability, a circuit breaker, and the first 36-test unit suite plus production TTL policies. See [UPGRADE.md](UPGRADE.md) for the full audit-upgrade detail.

---

## v1 — Initial release

Single-cluster TiDB Cloud architecture for streaming OCPP telemetry from 20,000 chargers, with the three-tier agent memory (episodic / semantic / procedural) on a unified data substrate and the first diagnostic agent loop.
