-- ============================================================================
-- Migration v4 — Split verified counters from agent-authored counters
-- ============================================================================
-- The v3 derived-confidence refactor was structurally sound but leaked a
-- data-integrity failure that reproduced the anti-pattern it was built to
-- kill: the single `confirmations` counter conflated THREE different sources
--
--   (a) consolidation_job seeding      (confirmations = len(group))
--   (b) near-duplicate merges          (write_fleet_memory Case 1)
--   (c) field verification             (verify_outcome — the intended source)
--
-- so the shortcut gate `confirmations >= 3` fired on pure self-corroboration:
-- agent consensus, not ground truth. The symmetric conflation existed on
-- `contradictions` (agent-authored supersede events could decay a genuinely
-- field-verified memory below the gate).
--
-- v4 splits BOTH counters. Only verify_outcome (field ground truth) may ever
-- write the `verified_*` counters; agent/consolidation/merge churn lands in
-- the informational `corroborations` / `supersede_events` columns, which do
-- NOT gate the shortcut and do NOT feed the Beta posterior.
--
-- The pre-v4 `confirmations` / `contradictions` columns are RETAINED as frozen
-- history (their values are preserved into corroborations / supersede_events by
-- the repair backfill); no decision path reads them after this migration.
--
-- Apply against the same database as schema.sql / schema_v3. Idempotency is
-- not assumed (ALTER ... ADD COLUMN errors if the column already exists); run
-- once per environment.
-- ============================================================================

ALTER TABLE fleet_memory
  ADD COLUMN verified_confirmations INT NOT NULL DEFAULT 0
    COMMENT 'Confirmations from verify_outcome (field ground truth) ONLY. Gates shortcut.',
  ADD COLUMN verified_contradictions INT NOT NULL DEFAULT 0
    COMMENT 'Contradictions from verify_outcome (field ground truth) ONLY. Feeds posterior.',
  ADD COLUMN corroborations INT NOT NULL DEFAULT 0
    COMMENT 'Agent/consolidation/merge corroboration. Informational, does NOT gate shortcut.',
  ADD COLUMN supersede_events INT NOT NULL DEFAULT 0
    COMMENT 'Agent-authored supersede/churn events. Informational, does NOT feed posterior.';

-- Backfill: recompute BOTH verified counters purely from agent_reasoning.verified_outcome.
-- Run the source_refs repair (Task 2) FIRST — the backfill depends on refs being joinable.
-- This file documents the intended final state; the repair script performs the ordered
-- execution.
--
--   verified_confirmations  = COUNT(DISTINCT linked agent_reasoning rows whose
--                             verified_outcome = 'fixed_as_diagnosed')
--   verified_contradictions = COUNT(DISTINCT linked agent_reasoning rows whose
--                             verified_outcome IN ('different_fault','no_fault_found'))
--   corroborations          = OLD confirmations   (preserved as informational history)
--   supersede_events        = OLD contradictions  (preserved as informational history)
--   confidence              = derive_confidence(provenance,
--                                               verified_confirmations,
--                                               verified_contradictions)
--
-- Execution is performed by migrations/repair_v4_source_refs.py (phase 2), which
-- runs AFTER the source_refs flattening (phase 1) so the DISTINCT ref joins are
-- reachable. The verified counters are recomputed PURELY from verified_outcome —
-- the pre-v4 confirmations/contradictions values are NOT max'd in, because those
-- values are exactly the polluted quantity this migration removes.
