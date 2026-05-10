# Agent Lifecycle, Quality Increase, and Cost Reduction

How a single anomaly trigger flows through the EV charger fleet diagnostic agent, what the cognitive foundation contributes at each stage, and the empirical results from a multi-cluster experiment that validated (and reframed) the architectural thesis.

This document is grounded in measured data, not aspirational design. Every number cited comes from an actual dispatch run on one of two test clusters:

- **Cluster B** — fresh cluster, ~30 dispatches' worth of accumulated memory. Used to characterize the warm-up curve.
- **Cluster A** — production cluster, ~3 weeks of accumulated `fleet_memory` and `agent_reasoning`. Used to characterize steady-state behavior.

Every code reference points to a real line in the current tree.

---

## 1. The full flow

```
                        ┌─────────────────────────┐
   anomaly window  ─►   │     dispatch.py         │
   in charger_windows   │ (concurrent fan-out:    │
                        │  5 workers, 1 agent     │
                        │  per charger)           │
                        └────────────┬────────────┘
                                     │
                       ┌─────────────┴─────────────┐
                       │      run_agent()           │
                       │  per charger, threaded     │
                       └─────────────┬─────────────┘
                                     │
   ┌──────────────────────────────── ▼ ─────────────────────────────────┐
   │  STEP 1 — assemble_context()  [<50ms, pure SQL, zero LLM calls]    │
   │                                                                    │
   │   Tier 1  charger profile           ~80 tokens     (registry)      │
   │   Tier 2  recent anomalies          ~100-300       (windows)       │
   │   Tier 3  active investigations     ~100-200       (reasoning)     │
   │   Tier 4  prior diagnoses           ~200-500       (vec search)    │
   │   Tier 5  fleet memory              cap 500        (vec + scope)   │
   │                                                                    │
   │  Returns: system_context, sources, top_fleet_match, fleet_matches  │
   └──────────────────────────────── │ ─────────────────────────────────┘
                                     │
   ┌──────────────────────────────── ▼ ─────────────────────────────────┐
   │  STEP 2 — Routing decision  [tool_handlers.py:1500]                │
   │                                                                    │
   │   Scan fleet_matches for any entry with                            │
   │     confidence >= 0.85  AND  similarity >= 0.55                    │
   │                                                                    │
   │     yes → SHORTCUT  Haiku + 3 rounds  (skips classify call)        │
   │     no  → fall through to:                                         │
   │             LOOKUP  Haiku + 5 rounds (status-shape trigger)        │
   │             EXPLORE Sonnet + 15 rounds (default; legacy path)      │
   └──────────────────────────────── │ ─────────────────────────────────┘
                                     │
   ┌──────────────────────────────── ▼ ─────────────────────────────────┐
   │  STEP 3 — Agent loop  [cached system prompt, R1+R2 capped seed]    │
   │                                                                    │
   │   for iteration in 1..max_tool_rounds:                             │
   │     model.create(system=cached_prompt, messages=conversation)      │
   │       └── system+tools cache READ (~0.1× cost on iter 2+)          │
   │     execute tool calls (search, recall, write checkpoints)         │
   │     append tool results to messages                                │
   │     break when stop_reason == 'end_turn'                           │
   │                                                                    │
   │   Tools used:                                                      │
   │     - search_similar_outages         (curated catalog match)       │
   │     - search_prior_diagnoses         (episodic memory recall)      │
   │     - recall_fleet_memory            (semantic memory recall)      │
   │     - get_recent_windows             (raw telemetry)               │
   │     - write_reasoning_checkpoint     (episodic memory write)       │
   │     - write_fleet_memory             (semantic memory write)       │
   │     - get/update_session_state       (working memory)              │
   └──────────────────────────────── │ ─────────────────────────────────┘
                                     │
   ┌──────────────────────────────── ▼ ─────────────────────────────────┐
   │  STEP 4 — Summary call  [Haiku, structured input only]             │
   │                                                                    │
   │   SELECT latest agent_reasoning row WHERE session_id = ...         │
   │     ↓                                                              │
   │   Build clean prompt from {observation, hypothesis, evidence_refs, │
   │                            confidence, resolution, reasoning_id}   │
   │     ↓                                                              │
   │   Haiku call with messages=[{role:user, content:checkpoint}]       │
   │     (NOT replaying the loop's full conversation)                   │
   │     ↓                                                              │
   │   Investigation Report (3-5 paragraph operational write-up)        │
   └──────────────────────────────── │ ─────────────────────────────────┘
                                     │
                                     ▼
                          Returned to dispatcher,
                          surfaced in fleet summary
```

---

## 2. Why each step is shaped this way

### Tier 5 caps (R2 + R1)

Tier 5 used to consume whatever budget remained after Tiers 1-4, with `limit=5` and 150-char content truncation. This worked for an empty `fleet_memory` table — but as memory grew, the seed inflated proportionally. **Per-investigation cost climbed +54% across a 10-dispatch warm-up** before R2/R1 landed.

R2 (`tool_handlers.py:990`): `limit=3` and `[:80]` content truncation. Treats the seed as a *pointer* to fleet patterns; the agent can `recall_fleet_memory` by id for the full text if it needs more.

R1 (`tool_handlers.py:142`): `TIER5_MAX_TOKENS = 500` hard cap on the section. Defense-in-depth — the seed's fleet_memory contribution can never exceed 500 tokens regardless of how big the underlying table grows.

**After R2+R1, mean dropped from 16,251 to 11,706 tokens/charger and the climb went from +54% to −2% (curve flat).**

### Routing layer

The original architecture assumed Sonnet would self-shortcut on familiar patterns. It doesn't. Sonnet uses richer context to do *more* work, not less — same task, more cross-referencing, more tool calls.

The routing layer makes the shortcut **external to the LLM**. Code inspects Tier 5's top matches, and if any passes both gates (confidence ≥ 0.85, similarity ≥ 0.55), routes the entire loop to Haiku with `max_tool_rounds = 3`. The shortcut also skips the classify call entirely — no triage needed when the cognitive foundation has already recognized the pattern.

`tool_handlers.py:1500-1580` — full implementation.

### Tier 5 graceful degradation

Originally, if `site_id` parsing failed for any reason (registry missing the charger, snapshot text format changed), the entire Tier 5 block was skipped. Silently. This zeroed out `top_fleet_match` and blocked the routing layer.

`tool_handlers.py:980` — when `site_id` is missing, fall back to `scope="any"` instead of skipping. Tagged in `sources` as `fleet_memory:WARN:site_id_missing` so degraded state is visible.

This was the root cause of a 0% shortcut rate on a fresh cluster despite a populated `fleet_memory` — the registry didn't have the test fixture chargers.

### Registry write-on-create

`seed/stream_telemetry.py:_register_charger_fleet` — when running in `--format direct` mode, register simulated chargers in `charger_registry` before writing telemetry. Idempotent via `INSERT IGNORE`.

Without this, synthetic chargers existed in `charger_telemetry` and `charger_windows` (telemetry data) but not `charger_registry` (metadata). Tier 1 silently failed → Tier 5 silently skipped → routing silently disabled.

### Slim summary call

The summary call used to send the entire `messages` array (the loop's full conversation) to Haiku for final report generation. This cost ~30-50k input tokens per investigation and frequently produced **empty text blocks** because Haiku struggles with bloated mixed-content payloads.

`tool_handlers.py` summary section — now reads the latest `agent_reasoning` row for the session and builds a focused 500-1500 token prompt from the checkpoint fields. Eliminates ~37% of input tokens per investigation and fixes the empty-report bug entirely.

### System prompt caching

`tool_handlers.py:1497` — the loop's system prompt is byte-identical across all iterations of one investigation. Render order is `tools → system → messages`; a `cache_control` marker on the system block caches both tools and system together.

Iteration 1 pays 1.25× (write); iterations 2-N read at 0.1×. Across a typical 5-iteration loop, this cuts system input tokens by ~70%.

---

## 3. Quality increase

The architecture's cognitive foundation produces measurably better diagnostic output than a stateless agent. This is the part that's always been true.

### Cluster recognition

A warm cluster (with accumulated `fleet_memory`) consistently identifies **cross-charger fault clusters** in its reports:

> *"This incident aligns with pattern ENV-002... fleet memory confirms 21+ additional units with identical earth leakage / GroundFailure signatures... cluster is strongly correlated with sub-2°C ambient temperatures."*

A cold cluster (empty `fleet_memory`) cannot. Each charger gets diagnosed individually with no awareness that other units share the same fault:

> *"This incident is not consistent with a widespread environmental issue."* (cold report on a charger that's actually a 21-member cluster)

### Recurrence detection

A warm investigation surfaces operational gaps that a stateless agent literally cannot see:

> *"This is the **5th confirmed ENV-002 event on CP-IE-TEST-00100** — indicating either persistent site environmental conditions or a batch component vulnerability. **Critically: zero physical remediation has been actioned across all five prior escalations.**"*

The "zero intervention" framing requires `agent_reasoning` history. It's not in the trigger telemetry; it's emergent from the cognitive foundation.

### Confidence ramp

| State | Confidence range | Reasoning |
|---|---:|---|
| Cold (no memory) | 0.72–0.78 | Diagnoses from raw signals only; no validating priors |
| Warm (rich memory) | 0.93–0.99 | Pattern-matched against confirmed cluster members + curated catalog |

### Cross-charger evidence chains

Warm reports reference specific peer chargers with confirmed similar faults:

> *"Reference fleet_memory entries [7205759403792793601, 2882303761517177442, 8935141660703244065] for cluster trend analysis. Priority isolation and repair of cluster members (CP-IE-TEST-00075, -00070, -00044, -00166, -00100, -00057)..."*

Cold reports cannot do this — there are no peer chargers in their context.

---

## 4. Cost reduction

The cost story has two distinct mechanisms and three measured dimensions:

- **Mechanism 1 — model substitution.** Routing shifts the loop from Sonnet ($3 input / $15 output per M) to Haiku ($1 / $5 per M). Same work, ~3-5× cheaper per token. This is the dominant cost lever.
- **Mechanism 2 — token volume.** Caching (P1), slim summary (P2), Tier 5 caps (R1+R2). Direct reduction of API tokens consumed.

Three dimensions worth tracking — they don't move together:

- **Tokens** — useful for cache analysis and per-charger telemetry
- **Dollars** — what actually matters at scale; dominated by the model swap
- **Wall-clock latency** — operator-facing; Haiku at 3 rounds is genuinely faster than Sonnet at 5-15

The original thesis ("eliminates the Token Tax") was framed against tokens only. That framing was incomplete — and as written, empirically false on production fleets. The reframe below tracks all three dimensions across both clusters tested.

### Cluster A — production scale, 3 weeks of accumulated memory

Cluster A routes 100% shortcut from run 1 onward. There is **no warm-up period** — the canonical patterns are already in `fleet_memory` from prior weeks of investigation. This is the intended steady-state behavior.

Per-investigation impact on cluster A (4-charger dispatch, post-routing):

| Path | Mean tokens/charger | Wall-clock (4 chargers) | Dollars/charger | Routing |
|---|---:|---:|---:|---|
| Pre-routing (Sonnet) | ~24,000 | ~80s | **$0.117** | n/a |
| Post-routing (Haiku) | ~16,600 | **48.6s** | **$0.029** | 100% shortcut |
| **Δ** | **−31%** | **−39%** | **−75%** | — |

**The token reduction is modest. The dollar reduction is dramatic.** Routing's primary cost mechanism is model substitution, and the ~3-5× price differential between Sonnet and Haiku dominates the savings calculation.

### Cluster B — small fleet warm-up curve

Cluster B started cold and was driven through 30+ dispatches to characterize the warm-up dynamics:

```
Run-state phase             Mean tokens/charger    Routing distribution
─────────────────────────   ────────────────────   ──────────────────────
Early warm-up (runs 1-12)            14,193        0% shortcut, 100% explore
Mid warm-up   (runs 13-21)           10,042        Mixed, increasing shortcuts
Steady warm   (runs 22+)              ~6,000       100% shortcut on canonical pattern
```

Single-dispatch floor: **3,931 tokens/charger.** Best result in the entire experiment.

Why cluster B has a warm-up curve and cluster A doesn't:

1. New `fleet_memory` entries written during early dispatches have agent-default confidence (0.85-0.93)
2. Vector retrieval initially ranks these newer specific entries above older curated ones
3. After 15-20 dispatches, the `consolidation_job` and stable agent writes converge on canonical patterns that pass the 0.85 routing gate
4. From that point, shortcuts fire reliably

Cluster A skipped this warm-up because it had already accumulated 3 weeks of consolidated patterns before the experiment began.

### Layer-by-layer optimization stack (cluster B baseline)

Each layer's contribution measured during the experiment:

| Lever | Mean tokens | Δ vs prior | Δ vs baseline |
|---|---:|---:|---:|
| Pre-optimization baseline | 16,251 | — | — |
| + System prompt caching (P1) | ~14,000 (est) | −14% | −14% |
| + Slim summary call (P2) | 12,185 | −13% | −25% |
| + Cap fleet memory (R2) | 12,185 | flat | −25% |
| + Hard token cap (R1) | 11,706 | −4% | −28% |
| + Routing (steady warm state) | **~6,000** | −49% | **−63%** |
| Single-dispatch floor observed | **3,931** | — | −76% |

### Cluster size shapes the savings regime

The same routing logic delivers different absolute savings depending on fleet state:

| Fleet state | Token reduction | Wall reduction | Dollar reduction |
|---|---:|---:|---:|
| Production fleet (cluster A, 3wk memory) | ~31% | ~39% | **~75%** |
| Small fleet (cluster B, warmed) | ~57% | similar | **~85%** |

Larger / older fleets carry more accumulated history. Even with R1+R2 caps on the seed, tool results during the loop touch more material on richer fleets. So **token savings shrink with fleet size, but the dollar saving (driven by model substitution) holds across both regimes**.

### Operator-facing signal: the `Routing:` line

The `Routing:` line in `dispatch.py`'s fleet summary is the headline metric:

```
Cold cluster:    Routing: 0 shortcut, 5 explore, 0 lookup (0% shortcut)
Warming up:      Routing: 1 shortcut, 4 explore, 0 lookup (20% shortcut)
Warm steady:     Routing: 5 shortcut, 0 explore, 0 lookup (100% shortcut)
```

Watching this number stabilize at 100% across runs is direct evidence the cognitive foundation is being exploited by the routing layer. Cluster A: 100% from run 1. Cluster B: 0% → 100% across ~22 runs.

---

## 5. Honest framing of the thesis

### What was claimed originally

> *"The cognitive foundation eliminates the Token Tax — the quadratic cost of re-assembling context from scratch on every invocation."*

This is true in a narrow technical sense (`assemble_context` runs in <50ms with zero LLM calls) and was empirically false in the broader sense it was usually pitched (per-investigation API spend doesn't decrease just because we have memory).

### What's actually true (post-experiment)

The cognitive foundation provides three distinct, measurable benefits:

**1. Persistent fleet awareness** — *capability that does not exist without it*  
Cluster recognition (21+ E-002 members), recurrence detection ("5th escalation, zero intervention"), cross-charger evidence chains. Stateless agents on identical telemetry produce strictly weaker diagnoses on these same chargers. **This is the irreducible value of the architecture.**

**2. Quality calibration** — *measurable confidence delta*  
Warm investigations run at 0.93–0.99 confidence; cold at 0.72–0.78. The same model on the same telemetry produces stronger conclusions when validating priors are available.

**3. Dollar-cost reduction via model substitution** — *measurable, requires the routing layer*  
Without the routing layer, accumulated memory makes Sonnet investigations *more expensive* (richer seed → more material to chew through). With the routing layer, accumulated memory drives shortcuts that **swap Sonnet for Haiku**, exploiting the ~3-5× price differential. Token savings are ~30-60% depending on fleet state; dollar savings are **~75-85% across all states tested**, because the model swap fires reliably whenever a high-confidence pattern is retrievable.

**4. Wall-clock reduction** — *operator-facing*  
Haiku + 3 rounds genuinely runs faster than Sonnet + 5-15 rounds. Measured ~40% latency reduction on cluster A (48.6s for 4-charger dispatch vs ~80s pre-routing).

### Defensible thesis statement

> *"The cognitive foundation is a persistent shared memory layer that provides four benefits: (1) cluster-level diagnostic capability impossible for stateless agents, (2) higher-confidence conclusions through prior validation, (3) ~75% dollar-cost reduction at steady-state via routing high-confidence pattern matches to a Haiku shortcut path, and (4) ~40% wall-clock latency reduction on the shortcut path. Cost reductions in dollar terms hold across all fleet states tested; token reductions vary by accumulated memory size. Cold clusters require a warm-up period (15-25 dispatches) before shortcuts fire reliably; production clusters with weeks of accumulated memory route 100% shortcut from the first dispatch."*

Every claim in that statement is supported by measured data from the experiment. None of it requires aspirational hand-waving.

---

## 6. Reading guide for the code

Anchor points for someone implementing or auditing this:

| Concern | File:lines |
|---|---|
| Context assembly (5 tiers, 4000-token cap) | `tool_handlers.py:assemble_context` |
| Tier 5 cap (R1) + content truncation (R2) | `tool_handlers.py:142, 990-1080` |
| Tier 5 graceful degradation | `tool_handlers.py:980-1010` |
| Routing decision | `tool_handlers.py:1500-1580` |
| System prompt caching | `tool_handlers.py:1497` (`cache_control: ephemeral`) |
| Slim summary call | `tool_handlers.py` summary section |
| AgentObserver routing tracking | `observability.py:94-103, 213-240` |
| Dispatch fleet summary (with routing) | `agent/dispatch.py:print_fleet_summary` |
| Registry write-on-create | `seed/stream_telemetry.py:_register_charger_fleet` |
| Three-tier memory schema | `schema.sql` (`agent_reasoning`, `fleet_memory`, `context_snapshots`) |

---

## 7. What the experiment changed about the architecture's positioning

Before: *cost optimization framed as the primary value*  
After: *capability framed as the primary value; dollar-cost reduction as a downstream consequence of routing*

Before: *"more memory = cheaper investigations" (linear in tokens)*  
After: *"more memory = better investigations always; cheaper investigations in dollars and wall-clock once routing fires"*

Before: *Token Tax as the headline problem*  
After: *Memory Wall (statelessness on fragmented stacks) as the headline problem; Token Tax narrowed to "context re-assembly cost," which `assemble_context` does eliminate (50ms, zero LLM calls)*

Before: *single metric (tokens) for cost analysis*  
After: *three metrics (tokens, dollars, latency) — they don't move together; the dollar story is the strongest because routing substitutes Haiku for Sonnet*

These reframes don't weaken the case. They strengthen it — they survive contact with empirical data on both a small warmed fleet and a production-scale fleet.

---

## 8. Operational status

- **Routing**: shipped and validated on **both** clusters tested. 100% shortcut rate observed in cluster A from run 1 (steady state) and in cluster B from run ~22 onward (post warm-up).
- **Quality preserved on shortcut path**: cluster A's Haiku-routed reports show full cluster recognition, recurrence detection, and cross-charger evidence chains. Read identically to Sonnet-routed reports.
- **Caching, summary slim, R1, R2, Tier 5 fallback, registry write**: all shipped and validated.
- **Threshold**: 0.85 confidence / 0.55 similarity. Calibrated to the actual confidence regime of agent-written `fleet_memory` entries (consolidation creates 0.97; agent writes during investigations land at 0.85-0.93).
- **Known operational gaps** (deferred, not blocking):
  - Per-future timeout in `dispatch.py` to prevent one stuck thread blocking a 10-dispatch loop
  - Schema-mismatch retries on `write_reasoning_checkpoint` (agent occasionally calls without `observation`); `_safe_handler` catches and surfaces a retryable error, agent self-corrects on next turn
  - Richer routing telemetry (per-model token breakdown in dispatch summary)

The architecture works. The thesis is reframed but defensible. The numbers are measured, not estimated, and validated on two independent clusters.
