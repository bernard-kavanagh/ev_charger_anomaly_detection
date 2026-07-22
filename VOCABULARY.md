# Cognitive Foundation — Vocabulary

Canonical terms used across all cognitive foundation repositories. Use these consistently in READMEs, documentation, blog posts, and presentations.

## Architecture

| Term | Definition |
|---|---|
| **Cognitive Foundation** | The architectural principle: the database is the agent's cognitive substrate, not its storage layer. It maintains knowledge over time through structured memory and lifecycle management. |
| **Unified Data Substrate** | A single ACID-compliant cluster that serves as both the operational data store and the agent's memory. No separate vector store, no cache, no warehouse. One transaction boundary. |
| **Data Plane** | Domain-specific operational data: telemetry, transactions, events, and ground truth catalogs. The raw material the agent reasons against. |
| **Context Plane** | Three-tier agent memory that persists across sessions. Episodic, semantic, and procedural memory — maintained by the five custodial duties. |
| **Agent Layer** | Stateless LLM with tools. Disposable and ephemeral. The platform remembers on its behalf. |
| **Domain Adapter** | The pluggable configuration that defines a use case: schema mapping, window/aggregation config, anomaly weights, text banding rules, and seed catalog. The only part you write per domain. |

## Three-Tier Memory

| Term | Definition | Typical implementation |
|---|---|---|
| **Episodic Memory** | Time-stamped records of what happened — interactions, investigations, decisions, outcomes. The agent's experiential history. | `agent_reasoning`, `chat_history` |
| **Semantic Memory** | Learned knowledge that persists across sessions and agents. Facts, patterns, and rules extracted from experience. Scoped (global, site, model, entity). | `fleet_memory`, `sales_knowledge`, knowledge base tables |
| **Procedural Memory** | Learned workflows and execution strategies. How the agent acts on what it knows — investigation playbooks, escalation logic, remediation procedures. | Branching + RCA logic, agent directives, escalation rules |

## Five Custodial Duties

| Duty | Definition |
|---|---|
| **Write Control** | Only confirmed outcomes are persisted. Working reasoning is ephemeral. Memory grows at O(investigations), not O(reasoning steps). |
| **Deduplication** | Near-duplicate memories (cosine distance < 0.15) are merged rather than accumulated. One strong memory with high evidence count, not ten weak duplicates. |
| **Reconciliation** | New evidence that contradicts existing memory auto-supersedes the older conclusion. `superseded_by` links the chain. Truth evolves, not accumulates. |
| **Confidence Decay** | `cleanup_job` decays the *derived* confidence of unreinforced memories on a monthly cadence, gated on `last_decayed_at` (a dedicated decay clock) rather than `updated_at` — so routine access-count writes no longer reset the clock. Memories that fall below 0.30 are auto-deprecated. Stale knowledge fades rather than poisoning. This is memory-store maintenance; it is distinct from the **Derived Confidence** mechanism (below) that computes the value being decayed. |
| **Compaction** | Periodic re-clustering merges memories that have drifted close together. Evidence counts consolidated. The knowledge store stays lean. |

## Key Mechanisms

| Term | Definition |
|---|---|
| **Context Assembly** | Budget-constrained function that builds the agent's prompt from priority-ordered sources. Runs before the model is invoked. Zero LLM calls. Pure SQL. The model never decides what to remember — the platform decides for it. |
| **Routing Layer** | After context assembly, code (not the model) inspects the top fleet memory match. If confidence and similarity gates are passed, the investigation routes to a Haiku shortcut path (3 tool rounds) instead of the default Sonnet exploration path (15 rounds). The cognitive foundation drives a model-selection decision, not just textual context. |
| **Shortcut Path** | The cheap, fast routing branch: Haiku model + 3 tool rounds + skipped classify call. Fires only when a fleet match clears a *structural* gate: derived `confidence ≥ 0.85` **AND** (`confirmations ≥ 3` **OR** `provenance = 'verified'`) **AND** `superseded_by IS NULL` **AND** `similarity ≥ 0.55`. The corroboration and supersession clauses defeat the self-report anti-pattern: the previous confidence-only gate could be tripped by a lone inflated number, and that number was the model's own self-report — so a model could talk its way onto the cheap path. The structural clauses require real, independent evidence. Produces ~75% dollar-cost reduction and ~40% latency reduction vs the explore path on production-scale fleets. |
| **Explore Path** | The default routing branch when no high-confidence match exists: Sonnet model + 15 tool rounds. Used on cold clusters or when the trigger doesn't match a stored pattern. The legacy code path before the routing layer was added. |
| **Warm-up Period** | The number of dispatches a fresh cluster needs before routing fires reliably. Empirically ~15-25 dispatches: enough for `consolidation_job` and stable agent writes to converge on canonical patterns that consistently surface as top vector matches. Production clusters with weeks of accumulated memory skip this entirely. |
| **Hybrid Search** | A *filter-and-rank* query, not a score blend. `FTS_MATCH_WORD` appears **bare** (never wrapped in another expression) in both `WHERE` and `SELECT`, against a **single full-text column per call** — TiDB forbids nesting it and forbids multiple full-text columns in one query. The keyword predicate in `WHERE` *filters*; vector cosine distance is the *sole ranker* in `ORDER BY`. Vectors catch meaning ("salt corrosion" ≈ "coastal earth leakage"); the keyword filter catches identifiers (error codes, firmware versions) that embeddings blur together. |
| **Semantic Banding** | Converting raw metrics to natural language before embedding. `voltage_stddev=12.3` → "high voltage variance, possible supply sag." Dramatically improves vector recall. |
| **Human-in-the-Loop** | The human validates, not executes. Serverless branching enables safe autonomy: agent proposes → branch validates → human approves → promote to production. |
| **Derived Confidence** | `derive_confidence(provenance, confirmations, contradictions)` is a pure Beta-posterior computation — no LLM call, no I/O. Priors encode a base rate before evidence: `session` (α=1, β=1, base 0.50), `consolidated` (α=3, β=1, base 0.75), `verified` (α=6, β=1, base ~0.86). The posterior mean is clamped to `[0.05, 0.99]` — it **never reaches 1.0**. This is the value stored in `confidence` and thresholded by routing. It is emphatically **not** `model_confidence`, a separate telemetry-only column that records the model's self-report and is never read by any decision path. |
| **Outcome Write-Back** | The closed ground-truth loop. `verify_outcome` (handler in `tool_handlers.py`, CLI in `agent/verify_outcome.py`) records a field-verified real-world result — `fixed_as_diagnosed`, `different_fault`, or `no_fault_found` — from a field tech or work order. The handler stamps `agent_reasoning.verified_outcome`, then propagates to every linked `fleet_memory` row via `JSON_CONTAINS(source_refs, …)`, incrementing `confirmations` (on a fix) or `contradictions` (otherwise) and re-deriving confidence. `verify_outcome` is deliberately **NOT** in the agent's tool list — the agent cannot verify its own diagnoses, because self-verification would recreate the self-report loop the derived-confidence design exists to break. |

## Problem Framing

| Term | Definition |
|---|---|
| **Memory Wall** | The infrastructure problem caused by stateless models on fragmented stacks. Not a model limitation — an architecture limitation. The headline problem the cognitive foundation solves. |
| **Token Tax** | Narrow definition: the runtime cost of re-assembling investigation context from scratch on every invocation. `assemble_context` eliminates this specifically — it runs in <50ms with zero LLM calls, returning a budget-constrained prompt regardless of how much fleet memory has accumulated. **Distinct from per-investigation API spend**, which is governed by the agent loop and the routing layer, not by context assembly. |
| **State Explosion** | The scaling problem when N users × M agents × Z branches creates thousands of concurrent memory contexts. Traditional databases assume one app, one database, predictable load. Agent workloads are the opposite. |

## Capability vs Cost

| Term | Definition |
|---|---|
| **Capability Multiplier** | The irreducible value of the cognitive foundation. Cluster recognition, recurrence detection, cross-charger evidence chains — outputs that stateless agents on identical telemetry literally cannot produce. Universal across cluster states; doesn't require routing or warm-up. |
| **Cost Reduction** | The downstream effect of routing high-confidence matches to the Haiku shortcut path. Operates in three measured dimensions (tokens, dollars, latency) that don't move together. The dollar reduction is the strongest claim (~75% on production fleets) because it's driven by Sonnet→Haiku model substitution, not just token volume. |
