"""
Unit Tests for Tool Handlers
==============================
Tests the core logic without requiring a live TiDB connection.
Uses mocks for database calls and embedding generation.

Run: python -m pytest tests/test_tool_handlers.py -v
"""

import json
import pytest
from unittest.mock import patch, MagicMock
from types import SimpleNamespace
from decimal import Decimal
from datetime import datetime


# ---------------------------------------------------------------------------
# Import targets
# ---------------------------------------------------------------------------

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tool_handlers import (
    TiDBEncoder, to_json, count_tokens, effective_budget,
    _extract_keywords, TOKEN_BUDGET_DEFAULT, TOKEN_SAFETY_MARGIN,
    _build_hybrid_sql, _build_vector_sql,
    _validate_identifiers, _hybrid_search,
    derive_confidence, _shortcut_eligible, verify_outcome,
    _build_summary_prompt, _build_degraded_summary_prompt,
    _flatten_source_refs, _accepted_merge_refs, _run_agent_loop,
    write_fleet_memory, write_reasoning_checkpoint,
    _validate_evidence_refs, _stage_routing, _gate_fail_reason,
    SHORTCUT_CONFIDENCE_MIN, SHORTCUT_CONFIRMATIONS_MIN,
)
from text_bander import (
    band_power, band_voltage, band_temperature, band_earth_leak,
    band_fan, band_errors, band_status_changes,
    compute_anomaly_breakdown, compute_anomaly_score,
    build_window_text,
)
from validation import validate_telemetry, validate_window


# ============================================================================
# TiDBEncoder
# ============================================================================

class TestTiDBEncoder:
    def test_decimal_serialisation(self):
        result = json.loads(to_json({"value": Decimal("3.14")}))
        assert result["value"] == 3.14
        assert isinstance(result["value"], float)

    def test_datetime_serialisation(self):
        dt = datetime(2026, 4, 3, 12, 30, 0)
        result = json.loads(to_json({"ts": dt}))
        assert result["ts"] == "2026-04-03T12:30:00"

    def test_nested_types(self):
        data = {
            "score": Decimal("0.750"),
            "ts": datetime(2026, 1, 1),
            "items": [Decimal("1.1"), Decimal("2.2")],
        }
        result = json.loads(to_json(data))
        assert result["score"] == 0.75
        assert result["items"] == [1.1, 2.2]


# ============================================================================
# Token Budget
# ============================================================================

class TestTokenBudget:
    def test_safety_margin_applied(self):
        budget = effective_budget(4000)
        assert budget == 3600  # 4000 * 0.90

    def test_safety_margin_custom_budget(self):
        budget = effective_budget(8000)
        assert budget == 7200

    def test_count_tokens_returns_positive(self):
        count = count_tokens("Hello, this is a test string.")
        assert count > 0
        assert isinstance(count, int)

    def test_count_tokens_scales_with_length(self):
        short = count_tokens("Hello")
        long = count_tokens("Hello " * 100)
        assert long > short


# ============================================================================
# Keyword Extraction (for hybrid search)
# ============================================================================

class TestKeywordExtraction:
    def test_extracts_error_codes(self):
        text = "Charger showing E-001 contactor weld failure pattern"
        keywords = _extract_keywords(text)
        assert "E-001" in keywords

    def test_extracts_fault_names(self):
        text = "Intermittent GroundFailure errors on coastal unit"
        keywords = _extract_keywords(text)
        assert "GroundFailure" in keywords

    def test_extracts_model_names(self):
        text = "Issue affects ABB Terra 54 units specifically"
        keywords = _extract_keywords(text)
        assert any("Terra" in k for k in keywords)

    def test_extracts_firmware_versions(self):
        text = "Bug in firmware version 3.1.2"
        keywords = _extract_keywords(text)
        assert "3.1.2" in keywords

    def test_extracts_environment_types(self):
        text = "Common in coastal environments"
        keywords = _extract_keywords(text)
        assert "coastal" in keywords

    def test_empty_for_generic_text(self):
        text = "The charger is not working properly"
        keywords = _extract_keywords(text)
        assert len(keywords) == 0


# ============================================================================
# Hybrid Search SQL Builder (TiDB FTS_MATCH_WORD)
# ============================================================================

class TestHybridSearchSQL:
    """Filter-and-rank (Variant B): FTS_MATCH_WORD appears BARE in SELECT
    and BARE in WHERE (never inside GREATEST/LEAST/arithmetic), ORDER BY
    references distance only, and placeholder count == parameter count.

    These shape assertions encode TiDB error 1221: FTS_MATCH_WORD must not
    be nested in any other function/expression in SELECT, and must have a
    matching FTS_MATCH_WORD in WHERE.
    """

    def _placeholder_count(self, sql):
        return sql.count("%s")

    # --- FTS_MATCH_WORD, never AGAINST -------------------------------------

    def test_uses_fts_match_word_not_against(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause", [],
        )
        assert "FTS_MATCH_WORD" in sql
        assert "AGAINST" not in sql
        assert "MATCH(" not in sql

    def test_ft_score_alias_present(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause", [],
        )
        assert "AS ft_score" in sql

    # --- FTS is never wrapped in another expression (error 1221) ----------

    def test_no_expression_wrapping_of_fts(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause",
            ["severity = %s"],
        )
        # The two forbidden wrappers from the broken refactor must be gone.
        assert "GREATEST(" not in sql
        assert "LEAST(" not in sql
        # ORDER BY ranks by distance only — no ft_score, no arithmetic.
        order_by = sql.split("ORDER BY", 1)[1]
        assert "distance ASC" in order_by
        assert "FTS_MATCH_WORD" not in order_by
        assert "ft_score" not in order_by

    def test_fts_appears_bare_in_select_and_where(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause", [],
        )
        # Exactly two FTS_MATCH_WORD calls: one in SELECT, one in WHERE.
        assert sql.count("FTS_MATCH_WORD") == 2
        assert "FTS_MATCH_WORD(%s, root_cause) AS ft_score" in sql
        where = sql.split("WHERE", 1)[1].split("ORDER BY", 1)[0]
        assert "FTS_MATCH_WORD(%s, root_cause)" in where

    # --- placeholder / parameter count parity -----------------------------

    def test_hybrid_placeholder_count_no_where(self):
        # 1 ft (SELECT) + 1 vec (SELECT) + 1 ft (WHERE) + 1 limit = 4
        sql = _build_hybrid_sql(
            "fleet_memory", "memory_vec", "content", [],
        )
        assert self._placeholder_count(sql) == 4

    def test_hybrid_placeholder_count_with_where(self):
        # base (4) + 2 where placeholders = 6
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause",
            ["severity = %s", "category = %s"],
        )
        assert self._placeholder_count(sql) == 6

    def test_hybrid_param_list_matches_placeholders(self):
        # Reproduce the exact param assembly from _hybrid_search and
        # assert it matches the number of %s in the built SQL.
        # Order: [query_text, query_vec, query_text, ...where..., limit].
        where_clauses = ["severity = %s"]
        params = ["safety"]
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", "root_cause", where_clauses,
        )
        query_vec, keyword_str, limit = "[0.1]", "E-001 GroundFailure", 5
        all_params = [keyword_str, query_vec, keyword_str] + params + [limit]
        assert len(all_params) == self._placeholder_count(sql)

    def test_vector_fallback_param_parity(self):
        sql = _build_vector_sql(
            "outage_catalog", "signature_vec", ["severity = %s"],
        )
        # 1 vec (SELECT) + 1 where + 1 limit = 3
        assert self._placeholder_count(sql) == 3
        all_params = ["[0.1]"] + ["safety"] + [5]
        assert len(all_params) == self._placeholder_count(sql)

    def test_vector_fallback_no_where(self):
        sql = _build_vector_sql("outage_catalog", "signature_vec", [])
        assert self._placeholder_count(sql) == 2  # vec + limit
        assert "WHERE" not in sql

    # --- allow-list guard --------------------------------------------------

    def test_disallowed_table_raises(self):
        with pytest.raises(ValueError):
            _build_hybrid_sql(
                "outage_catalog; DROP TABLE users", "signature_vec",
                "root_cause", [],
            )

    def test_disallowed_vec_column_raises(self):
        with pytest.raises(ValueError):
            _build_hybrid_sql(
                "outage_catalog", "signature_vec) --", "root_cause", [],
            )

    def test_disallowed_ft_column_raises(self):
        with pytest.raises(ValueError):
            _build_hybrid_sql(
                "outage_catalog", "signature_vec",
                "pattern_name", [],  # pattern_name has no FT index
            )

    def test_validate_identifiers_accepts_known(self):
        # Should not raise for the real call-site identifiers.
        _validate_identifiers("outage_catalog", "signature_vec", "root_cause")
        _validate_identifiers("fleet_memory", "memory_vec", "content")

    def test_vector_builder_also_validates(self):
        with pytest.raises(ValueError):
            _build_vector_sql("evil_table", "signature_vec", [])


class TestHybridSearchFallback:
    """_hybrid_search returns (rows, ft_used) and counts params correctly
    on both the hybrid and fallback paths, using a fake cursor."""

    class _FakeCursor:
        def __init__(self, raise_on_hybrid=False):
            self.raise_on_hybrid = raise_on_hybrid
            self.calls = []  # (sql, params)

        def execute(self, sql, params):
            self.calls.append((sql, params))
            if "FTS_MATCH_WORD" in sql and self.raise_on_hybrid:
                import pymysql
                raise pymysql.err.OperationalError(1105, "FTS not available")

        def fetchall(self):
            return [{"id": 1, "distance": 0.2}]

    def test_ft_used_true_on_success(self):
        cur = self._FakeCursor(raise_on_hybrid=False)
        rows, ft_used = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            "root_cause", "[0.1]",
            "charger showing E-001 fault", [], [], limit=5,
        )
        assert ft_used is True
        # Exactly one execute (the hybrid query) and params match placeholders
        sql, params = cur.calls[0]
        assert len(params) == sql.count("%s")

    def test_ft_used_false_on_pymysql_error(self):
        cur = self._FakeCursor(raise_on_hybrid=True)
        rows, ft_used = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            "root_cause", "[0.1]",
            "charger showing E-001 fault", [], [], limit=5,
        )
        assert ft_used is False
        # Two executes: failed hybrid, then vector fallback
        assert len(cur.calls) == 2
        fb_sql, fb_params = cur.calls[1]
        assert "FTS_MATCH_WORD" not in fb_sql
        assert len(fb_params) == fb_sql.count("%s")

    def test_empty_results_do_not_trigger_fallback(self):
        # HARD INVARIANT: a valid hybrid query that matches zero rows
        # returns (rows=[], ft_used=True) — it must NOT silently widen to
        # the vector-only path. ft_used=False is reserved for raised errors.
        class _EmptyCursor(self._FakeCursor):
            def fetchall(self):
                return []

        cur = _EmptyCursor(raise_on_hybrid=False)
        rows, ft_used = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            "root_cause", "[0.1]",
            "charger showing E-001 fault", [], [], limit=5,
        )
        assert rows == []
        assert ft_used is True, "empty FTS result is 'no matches', not a fallback"
        assert len(cur.calls) == 1, "no vector-only fallback query on empty result"
        assert "FTS_MATCH_WORD" in cur.calls[0][0]

    def test_no_keywords_skips_fulltext(self):
        cur = self._FakeCursor(raise_on_hybrid=False)
        rows, ft_used = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            "root_cause", "[0.1]",
            "the charger is not working", [], [], limit=5,
        )
        # Generic text yields no keywords → straight to vector-only
        assert ft_used is False
        assert len(cur.calls) == 1
        assert "FTS_MATCH_WORD" not in cur.calls[0][0]


# ============================================================================
# Derived Confidence (pure function)
# ============================================================================

class TestDeriveConfidence:
    def test_provenance_base_rates(self):
        # No evidence yet → prior base rate for each provenance.
        assert derive_confidence("session", 0, 0) == 0.50
        assert derive_confidence("consolidated", 0, 0) == 0.75
        # v4: verified prior shrank (6,1) -> (2,1); base ~0.67, not 0.86.
        assert derive_confidence("verified", 0, 0) == pytest.approx(0.67, abs=0.01)

    def test_monotonic_up_in_confirmations(self):
        prev = derive_confidence("session", 0, 0)
        for c in range(1, 20):
            cur = derive_confidence("session", c, 0)
            assert cur >= prev
            prev = cur

    def test_monotonic_down_in_contradictions(self):
        prev = derive_confidence("session", 5, 0)
        for c in range(1, 20):
            cur = derive_confidence("session", 5, c)
            assert cur <= prev
            prev = cur

    def test_confirmations_beat_contradictions(self):
        # More confirmations than contradictions → above the base rate.
        assert derive_confidence("session", 10, 1) > 0.50
        # The reverse → below it.
        assert derive_confidence("session", 1, 10) < 0.50

    def test_diminishing_returns(self):
        # Δ from 3→4 confirmations must be smaller than Δ from 0→1.
        delta_early = (derive_confidence("session", 1, 0)
                       - derive_confidence("session", 0, 0))
        delta_late = (derive_confidence("session", 4, 0)
                      - derive_confidence("session", 3, 0))
        assert delta_late < delta_early

    def test_never_reaches_one(self):
        # Even with absurd corroboration, derived confidence stays < 1.0.
        assert derive_confidence("verified", 500, 0) == 0.99
        assert derive_confidence("consolidated", 10_000, 0) < 1.0

    def test_clamps_floor(self):
        # Overwhelming contradictions floor out at 0.05, never negative/zero.
        assert derive_confidence("session", 0, 500) == 0.05

    def test_unknown_provenance_falls_back_to_session(self):
        assert derive_confidence("bogus", 0, 0) == derive_confidence("session", 0, 0)

    def test_returns_rounded_two_dp(self):
        val = derive_confidence("session", 2, 1)
        assert round(val, 2) == val


# ============================================================================
# Confidence self-report ratchet removed
# ============================================================================

class TestRatchetRemoved:
    def test_greatest_confidence_ratchet_absent(self):
        """The GREATEST(confidence, %s) self-report ratchet must be gone —
        confidence is re-derived from counters, never max'd upward."""
        here = os.path.dirname(os.path.abspath(__file__))
        src = os.path.join(os.path.dirname(here), "tool_handlers.py")
        with open(src) as f:
            text = f.read()
        assert "GREATEST(confidence" not in text


# ============================================================================
# SHORTCUT eligibility gate (pure helper)
# ============================================================================

class TestShortcutEligible:
    def _match(self, **overrides):
        # v4: eligibility gates on verified_confirmations, NOT the old
        # confirmations field and NOT provenance.
        base = {
            "id": 1,
            "confidence": 0.90,
            "similarity": 0.70,
            "verified_confirmations": 5,
            "verified_contradictions": 0,
            "provenance": "verified",
            "superseded_by": None,
        }
        base.update(overrides)
        return base

    def test_high_conf_high_verified_confirmations_accepted(self):
        assert _shortcut_eligible(self._match()) is True

    def test_high_conf_low_verified_confirmations_rejected(self):
        # Confidence passes but only 1 field confirmation.
        m = self._match(verified_confirmations=1)
        assert _shortcut_eligible(m) is False

    def test_verified_provenance_alone_not_enough(self):
        # v4: the removed OR-clause used to accept verified provenance with
        # zero field confirmations. It must now be REJECTED. Pinning this so
        # the OR-clause is never reintroduced.
        m = self._match(verified_confirmations=0, provenance="verified")
        assert _shortcut_eligible(m) is False

    def test_corroborations_do_not_gate(self):
        # A memory corroborated 50x by agents/merges but never field-verified
        # is NOT eligible, and its confidence sits at the base rate.
        m = self._match(
            confidence=derive_confidence("session", 0, 0),
            provenance="session",
            verified_confirmations=0,
            corroborations=50,
        )
        assert _shortcut_eligible(m) is False

    def test_verified_confirmations_absent_treated_as_zero(self):
        # Fail-closed: a match dict missing verified_confirmations is 0.
        m = self._match()
        del m["verified_confirmations"]
        assert _shortcut_eligible(m) is False

    def test_superseded_rejected(self):
        m = self._match(superseded_by=42)
        assert _shortcut_eligible(m) is False

    def test_low_confidence_rejected(self):
        m = self._match(confidence=0.80)
        assert _shortcut_eligible(m) is False

    def test_low_similarity_rejected(self):
        m = self._match(similarity=0.40)
        assert _shortcut_eligible(m) is False

    def test_bad_types_rejected(self):
        m = self._match(confidence="n/a")
        assert _shortcut_eligible(m) is False


# ============================================================================
# Golden values proving the derived-confidence fix (Task 4)
# ============================================================================

class TestConfidenceGoldenValues:
    """Pins the exact numbers that make the counter-split fix correct, using
    the v4 priors (verified=(2,1), consolidated=(3,1), session=(1,1)). These
    are decision-grade thresholds; a regression here silently re-opens the
    shortcut to unverified memories."""

    def _match(self, **kw):
        base = {"id": 1, "similarity": 0.70, "superseded_by": None}
        base.update(kw)
        return base

    def test_audited_memory_no_longer_eligible(self):
        # The audited memory post-fix: consolidated pattern, ZERO field
        # verifications. Derives to the consolidated base rate and does NOT
        # fire the shortcut (the whole point of the migration).
        conf = derive_confidence("consolidated", 0, 0)
        assert conf == 0.75
        m = self._match(confidence=conf, provenance="consolidated",
                        verified_confirmations=0)
        assert _shortcut_eligible(m) is False

    def test_single_verification_not_eligible(self):
        # Pins the removed OR-clause + the shrunk prior. A single field
        # confirmation on a verified-provenance memory: (1+2)/(1+3) = 0.75,
        # below 0.85, so NOT eligible.
        conf = derive_confidence("verified", 1, 0)
        assert conf == 0.75
        m = self._match(confidence=conf, provenance="verified",
                        verified_confirmations=1)
        assert _shortcut_eligible(m) is False

    def test_three_verifications_below_confidence_floor(self):
        # verified_confirmations=3: (3+2)/(3+3) = 0.833 -> 0.83, BELOW the
        # 0.85 confidence floor. The counter clause passes but confidence
        # does not, so still NOT eligible (the gate is conjunctive).
        conf = derive_confidence("verified", 3, 0)
        assert conf == pytest.approx(0.83, abs=0.005)
        assert conf < SHORTCUT_CONFIDENCE_MIN
        m = self._match(confidence=conf, provenance="verified",
                        verified_confirmations=3)
        assert _shortcut_eligible(m) is False

    def test_four_verifications_eligible(self):
        # verified_confirmations=4: (4+2)/(4+3) = 0.857 -> 0.86, clears 0.85.
        # Effectively 4 field confirmations are required for a verified memory
        # — intended, because the gate is conjunctive.
        conf = derive_confidence("verified", 4, 0)
        assert conf == pytest.approx(0.86, abs=0.005)
        assert conf >= SHORTCUT_CONFIDENCE_MIN
        assert 4 >= SHORTCUT_CONFIRMATIONS_MIN
        m = self._match(confidence=conf, provenance="verified",
                        verified_confirmations=4, similarity=0.70)
        assert _shortcut_eligible(m) is True
        # And the low-similarity side of the boundary fails.
        m_lowsim = self._match(confidence=conf, provenance="verified",
                               verified_confirmations=4, similarity=0.40)
        assert _shortcut_eligible(m_lowsim) is False

    def test_contradiction_symmetry(self):
        # verified_confirmations=4, verified_contradictions=2:
        # (4+2)/(6+3) = 0.667 -> 0.67, not eligible. supersede_events must NOT
        # enter the posterior (it is not even an argument to derive_confidence).
        conf = derive_confidence("verified", 4, 2)
        assert conf == pytest.approx(0.67, abs=0.005)
        m = self._match(confidence=conf, provenance="verified",
                        verified_confirmations=4, verified_contradictions=2,
                        supersede_events=99)
        assert _shortcut_eligible(m) is False


# ============================================================================
# verify_outcome input validation (no DB)
# ============================================================================

class TestVerifyOutcomeValidation:
    def test_bad_outcome_returns_error_without_db(self):
        # A patched _get_pool would raise if touched — validation must run
        # first and return before any DB access.
        with patch("tool_handlers._get_pool",
                   side_effect=AssertionError("DB must not be touched")):
            result = json.loads(verify_outcome(reasoning_id=1, outcome="bogus"))
        assert "error" in result
        assert result["tool"] == "verify_outcome"
        assert result["retryable"] is False

    def test_valid_outcomes_pass_validation(self):
        # Reach past validation into the DB layer; a patched _get_pool
        # confirms a valid outcome does proceed to DB access.
        for outcome in ("fixed_as_diagnosed", "different_fault", "no_fault_found"):
            with patch("tool_handlers._get_pool",
                       side_effect=RuntimeError("reached DB")) as _pool:
                result = json.loads(
                    verify_outcome(reasoning_id=1, outcome=outcome)
                )
            # _safe_handler wraps the RuntimeError into a structured error,
            # proving validation passed and the DB path was entered.
            assert "error" in result and "reached DB" in result["error"]


# ============================================================================
# Text Banding
# ============================================================================

class TestTextBanding:
    def test_power_zero(self):
        assert "no power" in band_power(0, 0)

    def test_power_high(self):
        result = band_power(45000, 48000, 50000)
        assert "near-maximum" in result or "high" in result

    def test_voltage_sag(self):
        result = band_voltage(205, 235, 3.0)
        assert "sag" in result.lower() or "210" in result

    def test_voltage_high_stddev(self):
        result = band_voltage(220, 240, 10.0)
        assert "variance" in result.lower() or "instability" in result.lower()

    def test_temperature_critical(self):
        result = band_temperature(72, 65)
        assert "critical" in result.lower() or "runaway" in result.lower()

    def test_earth_leak_dangerous(self):
        result = band_earth_leak(9.5)
        assert "dangerous" in result.lower() or "8mA" in result

    def test_fan_failure(self):
        result = band_fan(0, 30000)
        assert "failure" in result.lower() or "not spinning" in result.lower()

    def test_error_storm(self):
        result = band_errors(8, "GroundFailure, InternalError")
        assert "storm" in result.lower()

    def test_status_flapping(self):
        result = band_status_changes(12)
        assert "flapping" in result.lower()


# ============================================================================
# Anomaly Score Breakdown
# ============================================================================

class TestAnomalyBreakdown:
    def test_empty_for_normal_window(self):
        row = {"voltage_stddev": 2.0, "max_temp_c": 30, "error_count": 0,
               "max_earth_leak": 1.5, "status_changes": 2}
        breakdown = compute_anomaly_breakdown(row)
        assert len(breakdown) == 0

    def test_populates_voltage_instability(self):
        row = {"voltage_stddev": 10.0, "max_temp_c": 30, "error_count": 0,
               "max_earth_leak": 1.5, "status_changes": 2}
        breakdown = compute_anomaly_breakdown(row)
        assert "voltage_instability" in breakdown
        assert 0 < breakdown["voltage_instability"] <= 1.0

    def test_score_capped_at_1(self):
        row = {"voltage_stddev": 50, "max_temp_c": 100, "error_count": 20,
               "max_earth_leak": 30, "status_changes": 50}
        breakdown = compute_anomaly_breakdown(row)
        score = compute_anomaly_score(breakdown)
        assert score <= 1.0

    def test_score_matches_features(self):
        row = {"voltage_stddev": 10, "max_temp_c": 30, "error_count": 3,
               "max_earth_leak": 1, "status_changes": 2}
        breakdown = compute_anomaly_breakdown(row)
        score = compute_anomaly_score(breakdown)
        # Only voltage and error contribute
        assert "voltage_instability" in breakdown
        assert "error_rate" in breakdown
        assert "thermal_stress" not in breakdown
        assert score > 0

    def test_build_window_text_includes_banding(self):
        row = {
            "charger_id": "CP-IE-TEST-00001",
            "window_start": "2026-04-03T10:00:00",
            "window_end": "2026-04-03T10:05:00",
            "avg_power_w": 0, "max_power_w": 0,
            "min_voltage_v": 205, "max_voltage_v": 235,
            "voltage_stddev": 10.0,
            "max_temp_c": 72, "avg_temp_c": 65,
            "max_earth_leak": 9.5,
            "avg_fan_rpm": 0, "error_count": 8,
            "status_changes": 12, "anomaly_score": 0.85,
            "distinct_errors": "GroundFailure, InternalError",
            "anomaly_flags": '["earth_leakage", "high_temperature"]',
        }
        text = build_window_text(row)
        # Should contain descriptive banding, not just raw numbers
        assert "no power" in text.lower()
        assert "critical" in text.lower() or "runaway" in text.lower()
        assert "dangerous" in text.lower()
        assert "storm" in text.lower()
        assert "flapping" in text.lower()


# ============================================================================
# Validation
# ============================================================================

class TestValidation:
    def test_valid_message_passes(self):
        msg = {
            "charger_id": "CP-IE-TEST-00001",
            "ts": "2026-04-03T10:00:00Z",
            "status": "Charging",
            "voltage_v": 230.0,
            "power_w": 45000,
            "current_a": 195.6,
            "temp_c": 42.0,
            "earth_leak_ma": 1.5,
            "fan_rpm": 2500,
            "error_code": "NoError",
        }
        result = validate_telemetry(msg)
        assert result.is_valid
        assert len(result.errors) == 0

    def test_rejects_negative_voltage(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Charging",
               "voltage_v": -5.0}
        result = validate_telemetry(msg)
        assert not result.is_valid
        assert any("voltage" in e for e in result.errors)

    def test_rejects_excessive_voltage(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Charging",
               "voltage_v": 600.0}
        result = validate_telemetry(msg)
        assert not result.is_valid

    def test_clamps_soc_over_100(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Charging",
               "soc_percent": 105}
        result = validate_telemetry(msg)
        assert result.is_valid
        assert result.cleaned["soc_percent"] == 100
        assert result.has_warnings

    def test_warns_power_zero_while_charging(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Charging",
               "power_w": 0}
        result = validate_telemetry(msg)
        assert result.is_valid
        assert result.has_warnings
        assert any("power_w=0" in w for w in result.warnings)

    def test_rejects_missing_charger_id(self):
        msg = {"ts": "2026-01-01", "status": "Charging"}
        result = validate_telemetry(msg)
        assert not result.is_valid

    def test_rejects_negative_power(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Available",
               "power_w": -100}
        result = validate_telemetry(msg)
        assert not result.is_valid

    def test_clamps_negative_earth_leak(self):
        msg = {"charger_id": "CP-01", "ts": "2026-01-01", "status": "Available",
               "earth_leak_ma": -0.5}
        result = validate_telemetry(msg)
        assert result.is_valid
        assert result.cleaned["earth_leak_ma"] == 0.0

    def test_window_validation_clamps_score(self):
        window = {"charger_id": "CP-01", "window_start": "2026-01-01",
                  "window_end": "2026-01-01", "anomaly_score": 1.5}
        result = validate_window(window)
        assert result.is_valid
        assert result.cleaned["anomaly_score"] == 1.0


# ============================================================================
# Summary prompt budget
# ============================================================================


class TestSummaryPromptBudget:
    """
    Guard against prompt drift re-truncating the Haiku final report.

    The shortcut-path report was cut off mid-sentence once the summary
    prompt started carrying a wide checkpoint payload against a fixed
    max_tokens ceiling. The token budget was raised, but the durable fix
    is keeping the *prompt* lean so output has room. This test locks that
    in: a realistically-sized checkpoint must still produce a prompt well
    under budget.
    """

    # Rough guard-grade estimate — good enough to catch drift, not for
    # accounting accuracy.
    @staticmethod
    def _estimated_tokens(text: str) -> int:
        return len(text) // 4

    @staticmethod
    def _realistic_checkpoint() -> dict:
        """A synthetic checkpoint sized like a real, evidence-heavy run."""
        return {
            "id": 84213,
            "charger_id": "CP-04821",
            "observation": (
                "Charger CP-04821 reported six consecutive failed session "
                "starts across a 40-minute window, each aborting during "
                "contactor engagement on connector B. " * 4
            ),
            "hypothesis": (
                "Contactor weld on connector B is preventing clean load "
                "engagement, consistent with the prior fleet pattern. " * 3
            ),
            "confidence": 0.82,
            "resolution": (
                "Dispatch a field technician to replace the connector B "
                "contactor assembly and re-run the self-test sequence. " * 3
            ),
            # Deliberately wide: the raw evidence list would blow the budget
            # if inlined whole — the builder is expected to bound it.
            "evidence_refs": json.dumps(
                [{"window_id": i, "anomaly_score": 0.9, "ref": f"win-{i}"}
                 for i in range(40)]
            ),
            "tags": json.dumps(
                ["contactor", "connector-b", "weld", "dispatch", "recurring"]
            ),
        }

    def test_prompt_under_budget(self):
        checkpoint = self._realistic_checkpoint()
        prompt = _build_summary_prompt(
            checkpoint,
            "CP-04821",
            "6 failed session starts on CP-04821 in 40 minutes",
        )
        assert self._estimated_tokens(prompt) < 1200

    def test_wide_evidence_is_bounded(self):
        """The wide evidence payload must not be inlined verbatim."""
        checkpoint = self._realistic_checkpoint()
        prompt = _build_summary_prompt(checkpoint, "CP-04821", "trigger")
        assert len(prompt) < len(checkpoint["evidence_refs"]) + 4000
        assert "truncated" in prompt

    def test_core_sections_preserved(self):
        """Observation, cause and recommended action must survive intact."""
        checkpoint = self._realistic_checkpoint()
        prompt = _build_summary_prompt(checkpoint, "CP-04821", "trigger")
        assert checkpoint["observation"] in prompt
        assert checkpoint["hypothesis"] in prompt
        assert checkpoint["resolution"] in prompt


# ============================================================================
# source_refs flattening (Task 2)
# ============================================================================

_REF_RE = __import__("re").compile(r"^[a-z_]+:\d+$")


class TestSourceRefsFlattening:
    def test_source_refs_are_flat_scalars(self):
        # A nested payload like the pre-v4 JSON_ARRAY_APPEND bug produced:
        # some scalars, some singleton nested arrays, some deeply nested.
        nested = [
            "agent_reasoning:1",
            ["agent_reasoning:2"],
            [["agent_reasoning:3"]],
            "agent_reasoning:1",           # duplicate -> deduped
        ]
        flat = _flatten_source_refs(nested)
        assert flat == ["agent_reasoning:1", "agent_reasoning:2",
                        "agent_reasoning:3"]
        # Every element is a scalar string matching the ref regex; none a list.
        for el in flat:
            assert isinstance(el, str)
            assert not isinstance(el, list)
            assert _REF_RE.match(el)
        # And it round-trips through JSON as a flat array (what the merge writes).
        assert json.loads(json.dumps(flat)) == flat

    def test_json_string_input_is_decoded_and_flattened(self):
        # DictCursor returns JSON columns as strings; the merge reads them back.
        raw = json.dumps(["agent_reasoning:10", ["agent_reasoning:11"]])
        assert _flatten_source_refs(raw) == ["agent_reasoning:10",
                                             "agent_reasoning:11"]

    def test_malformed_elements_dropped(self):
        messy = ["agent_reasoning:1", "not a ref", "AGENT:99", 42, None,
                 "agent_reasoning:", ":5"]
        assert _flatten_source_refs(messy) == ["agent_reasoning:1"]

    def test_none_and_empty(self):
        assert _flatten_source_refs(None) == []
        assert _flatten_source_refs([]) == []
        assert _flatten_source_refs("null") == []

    def test_repair_recovers_stringified_refs(self):
        # The corruption the cluster-2 dry-run actually found: stringified JSON
        # arrays as single string elements (not nested Python lists). The old
        # regex-only classifier dropped these; recovery must parse and keep
        # them. Also exercises the widened grammar (outage_catalog:E-002).
        from tool_handlers import _flatten_source_refs_with_stats
        row = [
            "agent_reasoning:1",
            '["agent_reasoning:2"]',
            '["outage_catalog:E-002"]',
        ]
        flat, stats = _flatten_source_refs_with_stats(row)
        assert flat == [
            "agent_reasoning:1",
            "agent_reasoning:2",
            "outage_catalog:E-002",
        ]
        assert stats["malformed_dropped"] == 0
        assert stats["recovered_from_strings"] == 2
        assert stats["nested_flattened"] == 0
        assert stats["unmatched_formats"] == []

    def test_widened_grammar_admits_non_numeric_ids(self):
        # outage_catalog:E-002-style refs must survive; the pre-v4 numeric-only
        # regex would have dropped them.
        assert _flatten_source_refs(["outage_catalog:E-002"]) == [
            "outage_catalog:E-002"]

    def test_deeply_stringified_layers_unwrap_to_fixpoint(self):
        # A doubly-stringified layer: '["[\"agent_reasoning:7\"]"]' — the inner
        # array is itself a stringified array. Recovery recurses until fixpoint.
        from tool_handlers import _flatten_source_refs_with_stats
        doubly = json.dumps([json.dumps(["agent_reasoning:7"])])
        flat, stats = _flatten_source_refs_with_stats([doubly])
        assert flat == ["agent_reasoning:7"]
        assert stats["recovered_from_strings"] == 2
        assert stats["malformed_dropped"] == 0

    def test_unparseable_ref_shaped_string_reported_not_silent(self):
        # A value that fails the ref pattern AND is not JSON is dropped, but a
        # ref-shaped one (contains ':') is surfaced in unmatched_formats.
        from tool_handlers import _flatten_source_refs_with_stats
        flat, stats = _flatten_source_refs_with_stats(
            ["agent_reasoning:1", "WEIRD:Format!!"])
        assert flat == ["agent_reasoning:1"]
        assert stats["malformed_dropped"] == 1
        assert "WEIRD:Format!!" in stats["unmatched_formats"]


# ============================================================================
# Provenance-safe merges (Task 3)
# ============================================================================

class TestMergeProvenanceSafe:
    class _FakeCursor:
        """Returns a scripted resolution per agent_reasoning id."""
        def __init__(self, resolutions):
            self.resolutions = resolutions  # {id: resolution}
            self._last = None

        def execute(self, sql, params):
            self._last = int(params[0])

        def fetchone(self):
            res = self.resolutions.get(self._last)
            return {"resolution": res} if res is not None else None

    def test_merge_rejects_escalated_ref(self):
        # ref 1 is escalated -> rejected; ref 2 is confirmed -> kept.
        cur = self._FakeCursor({1: "escalated", 2: "confirmed"})
        accepted = _accepted_merge_refs(
            cur, ["agent_reasoning:1", "agent_reasoning:2"]
        )
        assert "agent_reasoning:1" not in accepted
        assert accepted == ["agent_reasoning:2"]

    def test_merge_accepts_promoted_and_confirmed_only(self):
        cur = self._FakeCursor({
            10: "confirmed", 11: "promoted",
            12: "dismissed", 13: "escalated",
        })
        accepted = _accepted_merge_refs(cur, [
            "agent_reasoning:10", "agent_reasoning:11",
            "agent_reasoning:12", "agent_reasoning:13",
        ])
        assert accepted == ["agent_reasoning:10", "agent_reasoning:11"]

    def test_merge_rejects_ref_to_missing_checkpoint(self):
        cur = self._FakeCursor({})  # id 99 not present -> fetchone None
        assert _accepted_merge_refs(cur, ["agent_reasoning:99"]) == []


# ============================================================================
# Consolidation provenance invariant (Task 5)
# ============================================================================

class TestConsolidationInvariant:
    def test_consolidation_only_references_promoted(self):
        """Every ref consolidation writes must point at a checkpoint it is
        about to flip to 'promoted' in the same step."""
        from tool_handlers import consolidation_job

        confirmed_rows = [
            {"charger_id": f"CP-{i}", "site_id": "S1",
             "observation": "obs", "hypothesis": "hyp",
             "tags": json.dumps(["contactor", "weld"]), "id": i,
             "manufacturer": "ABB", "model": "Terra-54",
             "environment": "coastal"}
            for i in (101, 102, 103)
        ]

        captured = {"refs": None, "promoted_ids": None}

        class _FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                if sql.strip().startswith("SELECT") and "agent_reasoning" in sql:
                    self._mode = "select"
                elif "UPDATE agent_reasoning" in sql and "promoted" in sql:
                    captured["promoted_ids"] = list(params)

            def fetchall(self):
                return confirmed_rows

        class _FakeDB:
            def cursor(self):
                return _FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def _fake_write_fleet_memory(*, category, scope, content,
                                     source_refs, provenance, corroborations):
            captured["refs"] = source_refs
            return "{}"

        # Haiku synthesis: return a fake response object.
        fake_client = MagicMock()
        fake_client.messages.create.return_value = SimpleNamespace(
            content=[SimpleNamespace(type="text", text="pattern")],
            usage=SimpleNamespace(input_tokens=1, output_tokens=1),
        )
        # `anthropic` may not be installed in the test env; consolidation_job
        # does `import anthropic` internally, so inject a fake module.
        fake_anthropic = SimpleNamespace(Anthropic=lambda: fake_client)

        with patch.dict("sys.modules", {"anthropic": fake_anthropic}), \
             patch("tool_handlers.get_db", return_value=_FakeDB()), \
             patch("tool_handlers.write_fleet_memory",
                   side_effect=_fake_write_fleet_memory):
            consolidation_job()

        assert captured["refs"] is not None, "write_fleet_memory was not called"
        ref_ids = {int(r.split(":", 1)[1]) for r in captured["refs"]}
        promoted = set(captured["promoted_ids"])
        assert ref_ids == promoted == {101, 102, 103}


# ============================================================================
# Agent loop stop_reason handling (Task 6, F2)
# ============================================================================

def _resp(stop_reason, blocks=None):
    """Build a fake Anthropic response with a usage namespace."""
    return SimpleNamespace(
        stop_reason=stop_reason,
        content=blocks if blocks is not None else [
            SimpleNamespace(type="text", text="ok")
        ],
        usage=SimpleNamespace(input_tokens=10, output_tokens=5),
    )


def _tool_block(name="recall_fleet_memory", tool_id="t1", tool_input=None):
    return SimpleNamespace(type="tool_use", name=name, id=tool_id,
                           input=tool_input or {})


class _FakeClient:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = 0
        self.messages = SimpleNamespace(create=self._create)

    def _create(self, **kwargs):
        self.calls += 1
        return self._responses.pop(0)


class TestAgentLoopStopReason:
    def _run(self, responses, max_rounds=5):
        client = _FakeClient(responses)
        obs = MagicMock()
        messages = [{"role": "user", "content": "trigger"}]
        counts, degraded, reason = _run_agent_loop(
            client, "claude-haiku-4-5", [{"type": "text", "text": "sys"}],
            [], messages, max_rounds, obs,
        )
        return client, obs, messages, counts, degraded, reason

    def test_loop_terminates_on_end_turn(self):
        client, obs, messages, counts, degraded, reason = self._run(
            [_resp("end_turn")]
        )
        assert client.calls == 1
        assert degraded is False and reason is None
        # Assistant message appended, no tool calls executed.
        assert messages[-1]["role"] == "assistant"
        assert counts == {}

    def test_loop_continues_on_tool_use(self):
        responses = [
            _resp("tool_use", [_tool_block()]),
            _resp("end_turn"),
        ]
        with patch("tool_handlers.handle_tool_call",
                   return_value='{"memories": []}') as htc:
            client, obs, messages, counts, degraded, reason = self._run(responses)
        assert client.calls == 2
        assert degraded is False and reason is None
        htc.assert_called_once()
        assert counts == {"recall_fleet_memory": 1}
        # A tool_result user turn was appended after the tool_use turn.
        assert any(
            m["role"] == "user" and isinstance(m["content"], list)
            and m["content"] and m["content"][0].get("type") == "tool_result"
            for m in messages
        )

    def test_loop_retries_once_on_max_tokens(self):
        # First turn truncates, retry succeeds with end_turn -> not degraded.
        responses = [_resp("max_tokens"), _resp("end_turn")]
        client, obs, messages, counts, degraded, reason = self._run(responses)
        assert client.calls == 2  # original + one retry, no third
        assert degraded is False and reason is None

    def test_double_truncation_marks_degraded(self):
        responses = [_resp("max_tokens"), _resp("max_tokens")]
        client, obs, messages, counts, degraded, reason = self._run(responses)
        assert client.calls == 2  # retried exactly once
        assert degraded is True
        assert reason == "max_tokens_truncation"
        obs.circuit_breaker.assert_any_call("max_tokens_truncation",
                                            "claude-haiku-4-5")

    def test_refusal_marks_degraded(self):
        client, obs, messages, counts, degraded, reason = self._run(
            [_resp("refusal")]
        )
        assert client.calls == 1
        assert degraded is True
        assert reason == "refusal"
        obs.circuit_breaker.assert_called()

    def test_unexpected_stop_reason_marks_degraded(self):
        client, obs, messages, counts, degraded, reason = self._run(
            [_resp("something_new")]
        )
        assert degraded is True
        assert reason.startswith("unexpected_stop_reason")


# ============================================================================
# v5 verification linkage — scripted-DB harness
# ============================================================================
#
# A tiny SQL router that dispatches execute() to a handler by substring match,
# so the pure logic of write_fleet_memory / verify_outcome can be exercised
# without a live TiDB. First matching route wins; order specific → generic.

class _ScriptCursor:
    def __init__(self, routes, log):
        self.routes = routes          # list of (substr, fn(self, params))
        self.log = log                # shared [(sql, params)]
        self._rows = []
        self.rowcount = 0
        self.lastrowid = None

    def execute(self, sql, params=None):
        self.log.append((sql, params))
        self._rows = []
        self.rowcount = 0
        for substr, fn in self.routes:
            if substr in sql:
                fn(self, params)
                return

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _ScriptConn:
    def __init__(self, cur):
        self._cur = cur
        self.committed = False
        self.rolledback = False

    def begin(self):
        pass

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolledback = True

    def close(self):
        pass

    def cursor(self):
        return self._cur


class _ScriptDB:
    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def cursor(self):
        return self._cur


def _executed(log, needle):
    """True if any executed SQL contains needle."""
    return any(needle in sql for sql, _ in log)


def _params_for(log, needle):
    """Params of the first executed SQL containing needle (or None)."""
    for sql, params in log:
        if needle in sql:
            return params
    return None


class TestPendingRefStamped:
    """Task 2 / test_pending_ref_stamped_on_write."""

    def _env(self, routes):
        log = []
        cur = _ScriptCursor(routes, log)
        conn = _ScriptConn(cur)
        db = _ScriptDB(cur)
        pool = SimpleNamespace(connection=lambda: conn)
        patches = [
            patch("tool_handlers.embed", lambda text: [0.0] * 384),
            patch("tool_handlers.get_db", new=lambda: db),
            patch("tool_handlers._get_pool", new=lambda: pool),
        ]
        return log, cur, conn, patches

    def test_pending_ref_stamped_on_insert(self):
        routes = [
            ("AS distance", lambda c, p: setattr(c, "_rows", [])),  # no near-dup
            ("SELECT id FROM agent_reasoning WHERE session_id",
             lambda c, p: setattr(c, "_rows", [{"id": 777}])),
            ("INSERT INTO fleet_memory",
             lambda c, p: setattr(c, "lastrowid", 1)),
        ]
        log, cur, conn, patches = self._env(routes)
        for pt in patches:
            pt.start()
        try:
            result = json.loads(write_fleet_memory(
                category="pattern", scope="global",
                content="contactor weld pattern", source_refs=[],
                session_id="sess-A",
            ))
        finally:
            for pt in patches:
                pt.stop()

        assert result["status"] == "created"
        insert_params = _params_for(log, "INSERT INTO fleet_memory")
        # pending_refs is the 9th bind (index 8); adjudicated_refs the 10th.
        pending = json.loads(insert_params[8])
        adjudicated = json.loads(insert_params[9])
        assert pending == ["agent_reasoning:777"]
        assert adjudicated == []

    def test_pending_ref_stamped_on_merge(self):
        routes = [
            ("AS distance",
             lambda c, p: setattr(c, "_rows", [
                 {"id": 500, "category": "pattern", "distance": 0.05}])),
            ("SELECT source_refs, confidence, pending_refs",
             lambda c, p: setattr(c, "_rows", [{
                 "source_refs": json.dumps(["agent_reasoning:9"]),
                 "confidence": 0.7, "pending_refs": None}])),
            ("SELECT resolution FROM agent_reasoning",
             lambda c, p: setattr(c, "_rows", [{"resolution": "confirmed"}])),
            ("SELECT id FROM agent_reasoning WHERE session_id",
             lambda c, p: setattr(c, "_rows", [{"id": 777}])),
            ("UPDATE fleet_memory", lambda c, p: None),
        ]
        log, cur, conn, patches = self._env(routes)
        for pt in patches:
            pt.start()
        try:
            result = json.loads(write_fleet_memory(
                category="pattern", scope="global",
                content="contactor weld pattern",
                source_refs=["agent_reasoning:42"],
                session_id="sess-A",
            ))
        finally:
            for pt in patches:
                pt.stop()

        assert result["status"] == "updated_existing"
        update_params = _params_for(log, "UPDATE fleet_memory")
        # merge UPDATE binds: (content, source_refs, pending_refs, vec, id)
        pending = json.loads(update_params[2])
        assert "agent_reasoning:777" in pending      # session ref stamped
        # and it is a flat, deduped scalar list
        assert all(isinstance(x, str) for x in pending)
        assert len(pending) == len(set(pending))


class _VerifyEnv:
    """Helper to run verify_outcome against a scripted DB and return
    (result, log, conn)."""

    @staticmethod
    def run(checkpoint_row, candidate_rows, reasoning_id, outcome):
        log = []
        captured = {}

        def _checkpoint(c, p):
            c._rows = [checkpoint_row] if checkpoint_row is not None else []

        def _candidates(c, p):
            c._rows = list(candidate_rows)

        routes = [
            ("verified_outcome, verified_at", _checkpoint),
            ("SET verified_outcome", lambda c, p: None),
            ("JSON_CONTAINS(source_refs", _candidates),
            ("SET resolution = 'confirmed'", lambda c, p: None),
            ("SET verified_confirmations", lambda c, p: None),
        ]
        cur = _ScriptCursor(routes, log)
        conn = _ScriptConn(cur)
        pool = SimpleNamespace(connection=lambda: conn)
        with patch("tool_handlers._get_pool", new=lambda: pool):
            result = json.loads(verify_outcome(
                reasoning_id=reasoning_id, outcome=outcome))
        return result, log, conn


class TestVerifyPromotion:
    """Task 3 — promotion, idempotency, loud failure."""

    def test_verify_promotes_pending_ref(self):
        # test_verify_promotes_pending_ref: positive outcome moves ref
        # pending->source, +1 confirmation, escalated->confirmed, confidence
        # re-derived.
        checkpoint = {"verified_outcome": None, "verified_at": None,
                      "resolution": "escalated"}
        memory = {"id": 10, "provenance": "session",
                  "verified_confirmations": 0, "verified_contradictions": 0,
                  "source_refs": json.dumps([]),
                  "pending_refs": json.dumps(["agent_reasoning:5"]),
                  "adjudicated_refs": None}
        result, log, conn = _VerifyEnv.run(checkpoint, [memory], 5,
                                           "fixed_as_diagnosed")

        assert result["status"] == "ok"
        assert result["outcome_action"] == "stamped"
        assert len(result["touched_memories"]) == 1
        # Escalated checkpoint flipped to confirmed.
        assert _executed(log, "SET resolution = 'confirmed'")
        up = _params_for(log, "SET verified_confirmations")
        # binds: (vc, vcontra, provenance, confidence, source, pending, adj, id)
        vc, vcontra, prov, conf = up[0], up[1], up[2], up[3]
        source = json.loads(up[4])
        pending = json.loads(up[5])
        adjudicated = json.loads(up[6])
        assert vc == 1 and vcontra == 0
        assert prov == "verified"
        assert conf == derive_confidence("verified", 1, 0) == 0.75
        assert "agent_reasoning:5" in source        # moved into source_refs
        assert "agent_reasoning:5" not in pending    # removed from pending
        assert any(a["ref"] == "agent_reasoning:5"
                   and a["outcome"] == "fixed_as_diagnosed"
                   for a in adjudicated)

    def test_verify_propagates_previously_stamped_outcome(self):
        # test_verify_propagates_previously_stamped_outcome: a checkpoint with
        # verified_outcome ALREADY set but never propagated IS delivered to
        # memory. Guards against the idempotency mechanism blocking the repair.
        checkpoint = {"verified_outcome": "fixed_as_diagnosed",
                      "verified_at": "2026-07-01T00:00:00",
                      "resolution": "escalated"}
        memory = {"id": 10, "provenance": "session",
                  "verified_confirmations": 0, "verified_contradictions": 0,
                  "source_refs": json.dumps([]),
                  "pending_refs": json.dumps(["agent_reasoning:5"]),
                  "adjudicated_refs": None}
        result, log, conn = _VerifyEnv.run(checkpoint, [memory], 5,
                                           "fixed_as_diagnosed")

        assert result["status"] == "ok"                       # DID propagate
        assert result["outcome_action"] == "already_stamped_reprop"
        # NOT re-stamped (outcome already present)...
        assert not _executed(log, "SET verified_outcome")
        # ...but the memory WAS updated.
        assert _executed(log, "SET verified_confirmations")
        assert len(result["touched_memories"]) == 1

    def test_propagation_idempotent_per_ref(self):
        # test_propagation_idempotent_per_ref: a ref already in adjudicated_refs
        # is skipped; the second run is a no-op (no double increment).
        checkpoint = {"verified_outcome": "fixed_as_diagnosed",
                      "verified_at": "2026-07-01T00:00:00",
                      "resolution": "confirmed"}
        memory = {"id": 10, "provenance": "verified",
                  "verified_confirmations": 1, "verified_contradictions": 0,
                  "source_refs": json.dumps(["agent_reasoning:5"]),
                  "pending_refs": json.dumps([]),
                  "adjudicated_refs": json.dumps([
                      {"ref": "agent_reasoning:5",
                       "outcome": "fixed_as_diagnosed",
                       "at": "2026-07-01T00:00:00"}])}
        result, log, conn = _VerifyEnv.run(checkpoint, [memory], 5,
                                           "fixed_as_diagnosed")

        # No memory UPDATE at all — already counted.
        assert not _executed(log, "SET verified_confirmations")
        assert result["status"] == "ok_no_propagation"
        assert len(result["touched_memories"]) == 0
        assert len(result["skipped_memories"]) == 1

    def test_outcome_conflict_rejected(self):
        # test_outcome_conflict_rejected: a second verify with a DIFFERENT
        # outcome returns outcome_conflict and changes nothing.
        checkpoint = {"verified_outcome": "different_fault",
                      "verified_at": "2026-07-01T00:00:00",
                      "resolution": "escalated"}
        result, log, conn = _VerifyEnv.run(checkpoint, [], 5,
                                           "fixed_as_diagnosed")

        assert result["status"] == "outcome_conflict"
        assert result["existing_outcome"] == "different_fault"
        assert result["incoming_outcome"] == "fixed_as_diagnosed"
        # Nothing beyond the checkpoint lookup ran; the txn rolled back.
        assert not _executed(log, "SET verified_outcome")
        assert not _executed(log, "JSON_CONTAINS(source_refs")
        assert conn.rolledback is True

    def test_negative_adjudication_preserves_audit(self):
        # test_negative_adjudication_preserves_audit: negative outcome records
        # the ref in adjudicated_refs, +1 contradiction, does NOT add to
        # source_refs, and does NOT flip the escalated checkpoint.
        checkpoint = {"verified_outcome": None, "verified_at": None,
                      "resolution": "escalated"}
        memory = {"id": 10, "provenance": "verified",
                  "verified_confirmations": 2, "verified_contradictions": 0,
                  "source_refs": json.dumps([]),
                  "pending_refs": json.dumps(["agent_reasoning:5"]),
                  "adjudicated_refs": None}
        result, log, conn = _VerifyEnv.run(checkpoint, [memory], 5,
                                           "different_fault")

        assert result["status"] == "ok"
        up = _params_for(log, "SET verified_confirmations")
        vc, vcontra = up[0], up[1]
        source = json.loads(up[4])
        adjudicated = json.loads(up[6])
        assert vcontra == 1                          # contradiction counted
        assert vc == 2                               # confirmations untouched
        assert "agent_reasoning:5" not in source     # NOT promoted to evidence
        assert any(a["ref"] == "agent_reasoning:5"
                   and a["outcome"] == "different_fault"
                   for a in adjudicated)             # audit preserved
        # Negative outcome must NOT flip the escalated checkpoint.
        assert not _executed(log, "SET resolution = 'confirmed'")

    def test_ok_no_propagation_warns(self):
        # test_ok_no_propagation_warns: zero memories touched returns
        # ok_no_propagation with a warning.
        checkpoint = {"verified_outcome": None, "verified_at": None,
                      "resolution": "confirmed"}
        result, log, conn = _VerifyEnv.run(checkpoint, [], 5,
                                           "fixed_as_diagnosed")

        assert result["status"] == "ok_no_propagation"
        assert result["touched_memories"] == []
        assert "warning" in result and result["warning"]
        # The outcome WAS stamped on the checkpoint even though nothing
        # propagated (loud, not silent).
        assert _executed(log, "SET verified_outcome")
        assert result["outcome_action"] == "stamped"


class TestEvidenceRefAllowList:
    """Task 4 — write-time ref allow-list and quarantine."""

    class _NoSQLCursor:
        """Raises if any SQL runs — proves unknown prefixes are rejected
        BEFORE any query is constructed."""
        def execute(self, *a, **k):
            raise AssertionError("no SQL should run for an unknown prefix")

        def fetchall(self):
            raise AssertionError("no SQL should run for an unknown prefix")

    def test_ref_prefix_not_in_allowlist_quarantined(self):
        cur = self._NoSQLCursor()
        valid, quarantined = _validate_evidence_refs(cur, ["users:1"])
        assert valid == []
        assert quarantined == [{"ref": "users:1", "reason": "unknown_prefix"}]

    def test_known_prefix_existence_checked_and_quarantined(self):
        # agent_reasoning:1 exists, agent_reasoning:2 does not; a malformed ref
        # is quarantined without SQL. Confirms batched existence check + the
        # not_found / malformed reasons.
        log = []

        def _existence(c, p):
            # Only id 1 exists.
            c._rows = [{"pk": 1}]

        routes = [("FROM agent_reasoning", _existence)]
        cur = _ScriptCursor(routes, log)
        valid, quarantined = _validate_evidence_refs(
            cur, ["agent_reasoning:1", "agent_reasoning:2", "not-a-ref"])
        assert valid == ["agent_reasoning:1"]
        reasons = {q["ref"]: q["reason"] for q in quarantined}
        assert reasons["agent_reasoning:2"] == "not_found"
        assert reasons["not-a-ref"] == "malformed"

    def test_outage_catalog_uses_pattern_id(self):
        # outage_catalog:E-002 must be checked against pattern_id, not the
        # AUTO_RANDOM id.
        seen = {}

        def _existence(c, p):
            seen["params"] = p
            c._rows = [{"pk": "E-002"}]

        routes = [("FROM outage_catalog", _existence)]
        cur = _ScriptCursor(routes, [])
        valid, quarantined = _validate_evidence_refs(cur, ["outage_catalog:E-002"])
        assert valid == ["outage_catalog:E-002"]
        assert quarantined == []
        assert seen["params"] == ["E-002"]


class TestRoutingStages:
    """Task 5 — test_routing_log_stages: staged counts distinguish empty-fleet
    from gated."""

    def _match(self, **kw):
        base = {"id": 1, "confidence": 0.90, "similarity": 0.70,
                "verified_confirmations": 5, "superseded_by": None}
        base.update(kw)
        return base

    def test_empty_fleet(self):
        staged = _stage_routing([])
        assert staged["candidates"] == 0
        assert staged["similar"] == 0
        assert staged["eligible"] == 0
        assert staged["top_candidate"] is None
        assert staged["gate_fail_reason"] is None

    def test_similar_but_gated(self):
        # A populated fleet where the (single) candidate is similar but has no
        # field confirmations: candidates>0 distinguishes it from empty-fleet,
        # eligible=0 shows it was gated, and gate_fail_reason names why.
        gated = self._match(verified_confirmations=0)
        staged = _stage_routing([gated])
        assert staged["candidates"] == 1
        assert staged["similar"] == 1            # cleared the similarity floor
        assert staged["eligible"] == 0           # but gated
        assert staged["gate_fail_reason"] == "verified_confirmations<3"

    def test_eligible_candidate(self):
        staged = _stage_routing([self._match()])
        assert staged["eligible"] == 1
        assert staged["gate_fail_reason"] is None

    def test_gate_fail_reason_matches_clause_order(self):
        # superseded short-circuits before the numeric clauses.
        assert _gate_fail_reason(self._match(superseded_by=9)) == "superseded"
        assert _gate_fail_reason(
            self._match(confidence=0.5)) == "confidence<0.85"


class TestSummaryHedging:
    """Task 6 — anti-fabrication + confidence-calibrated hedging."""

    def _checkpoint(self, confidence):
        return {"id": 42, "observation": "voltage sag under load",
                "hypothesis": "cable resistance degradation",
                "confidence": confidence, "resolution": "confirmed",
                "evidence_refs": json.dumps(["charger_windows:1"])}

    def test_low_confidence_recommends_inspection(self):
        prompt = _build_summary_prompt(self._checkpoint(0.55), "CP-1", "alert")
        assert "inspection" in prompt.lower()
        assert "do not" in prompt.lower() or "not issue" in prompt.lower()
        # Anti-fabrication clause present.
        assert "traceable" in prompt.lower()
        assert "state its absence" in prompt.lower()

    def test_high_confidence_allows_remediation(self):
        prompt = _build_summary_prompt(self._checkpoint(0.90), "CP-1", "alert")
        assert "remediation recommendation is" in prompt.lower()
        assert "traceable" in prompt.lower()

    def test_degraded_prompt_is_anti_fabrication(self):
        prompt = _build_degraded_summary_prompt(
            None, "CP-1", "alert", "refusal")
        assert "traceable" in prompt.lower()
        assert "state its absence" in prompt.lower()
        assert "never a definitive remediation" in prompt.lower()
