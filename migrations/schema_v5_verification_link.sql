-- ============================================================================
-- Migration v5 — Explicit checkpoint→memory verification linkage
-- ============================================================================
-- Live testing after v4 proved the verification loop was severed. An escalated
-- checkpoint (agent_reasoning) merged into a fleet_memory, but the Task-3
-- provenance filter (_accepted_merge_refs) refused to graft the escalated
-- ref into source_refs. When the field tech later confirmed the diagnosis,
-- verify_outcome stamped the outcome on the checkpoint but touched ZERO
-- memories — no memory carried the ref. So verified_confirmations could never
-- increment and no memory could ever reach shortcut eligibility. The filter
-- that keeps UNVERIFIED escalations out of memory also locked VERIFIED ones out.
--
-- Root cause of the un-derivability: fleet_memory has no session column and
-- session_state has no back-reference, so checkpoint→memory linkage cannot be
-- reconstructed after the fact. It must be stamped EXPLICITLY at write time.
--
-- v5 adds two columns to carry that linkage independently of source_refs (which
-- remains model-supplied, evidence-bearing, and provenance-filtered):
--
--   pending_refs      — the platform-stamped linkage record. The current
--                       session's checkpoint ref is written here at
--                       write_fleet_memory time, ALWAYS, regardless of the
--                       checkpoint's resolution. It is NOT evidence: never fed
--                       to confidence derivation, never assembled into context,
--                       never shown to the model. It exists only so a later
--                       field adjudication can find the memory to propagate to.
--
--   adjudicated_refs  — the idempotency record + audit trail. Every counter
--                       movement verify_outcome makes against this memory is
--                       recorded here as {ref, outcome, at}. A ref present here
--                       has already been counted and must never be counted
--                       again — this is the per-memory/per-ref propagation
--                       idempotency mechanism, kept DISTINCT from the
--                       checkpoint-level verified_outcome idempotency.
--
-- On a positive field outcome verify_outcome MOVES the ref from pending_refs
-- into source_refs (it has now earned evidence status) and increments
-- verified_confirmations. On a negative outcome it removes the ref from
-- pending_refs but does NOT add it to source_refs; the audit trail in
-- adjudicated_refs is what preserves the fact that it was seen.
--
-- Apply against the same database as schema.sql / schema_v3 / schema_v4.
-- Idempotency is NOT assumed (ALTER ... ADD COLUMN errors if the column already
-- exists); run once per environment. There is deliberately NO backfill script:
-- the two legacy pre-v5 orphaned checkpoints are re-linked by hand via
-- `python agent/verify_outcome.py --link-memory <memory_id> ...` (Task 3).
-- ============================================================================

ALTER TABLE fleet_memory
  ADD COLUMN pending_refs JSON
    COMMENT 'Checkpoint refs awaiting field adjudication. Platform-stamped, never model-supplied. Never evidence, never fed to context assembly.',
  ADD COLUMN adjudicated_refs JSON
    COMMENT 'Refs whose field outcome has been COUNTED against this memory: [{ref, outcome, at}]. Idempotency record + audit trail for every counter movement.';
