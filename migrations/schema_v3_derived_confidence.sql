-- ============================================================================
-- Migration v3 — Derived confidence
-- ============================================================================
-- Decision-grade confidence is now computed by the platform from observable
-- events (corroboration + verified field outcomes), never taken from the
-- model's self-report. This migration:
--
--   * Adds model_confidence columns to hold the model self-report as
--     TELEMETRY ONLY — never read by a decision path again.
--   * Adds provenance + confirmation/contradiction counters that feed
--     derive_confidence() (see tool_handlers.py).
--   * Adds a dedicated decay clock (last_decayed_at) so confidence decay
--     no longer piggybacks on updated_at (which access-count writes bump).
--   * Adds verified_outcome / verified_at to agent_reasoning for the
--     field-tech write-back path (agent/verify_outcome.py -> verify_outcome).
--
-- The existing `confidence` column is RETAINED, but its meaning changes:
-- it now holds the platform-DERIVED confidence, recomputed by the platform
-- and never written from a model-supplied value again.
--
-- Apply against the same database as schema.sql. Idempotency is not
-- assumed (ALTER ... ADD COLUMN errors if the column already exists);
-- run once per environment.
-- ============================================================================

ALTER TABLE fleet_memory
  ADD COLUMN model_confidence DECIMAL(3,2) NULL COMMENT 'Model self-report, telemetry only — never used in decisions',
  ADD COLUMN provenance ENUM('session','consolidated','verified') NOT NULL DEFAULT 'session',
  ADD COLUMN confirmations INT NOT NULL DEFAULT 0,
  ADD COLUMN contradictions INT NOT NULL DEFAULT 0,
  ADD COLUMN last_decayed_at TIMESTAMP NULL;

ALTER TABLE agent_reasoning
  ADD COLUMN model_confidence DECIMAL(3,2) NULL COMMENT 'Model self-report, telemetry only',
  ADD COLUMN verified_outcome ENUM('fixed_as_diagnosed','different_fault','no_fault_found') NULL,
  ADD COLUMN verified_at TIMESTAMP NULL;

-- Backfill: preserve the pre-migration self-reported values as telemetry.
-- The `confidence` column itself is left as-is; it will be recomputed to a
-- derived value the next time each row is written/corroborated.
UPDATE fleet_memory   SET model_confidence = confidence WHERE model_confidence IS NULL;
UPDATE agent_reasoning SET model_confidence = confidence WHERE model_confidence IS NULL;
