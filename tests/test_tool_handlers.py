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
    _build_ft_expr, _build_hybrid_sql, _build_vector_sql,
    _validate_identifiers, _hybrid_search,
    derive_confidence, _shortcut_eligible, verify_outcome,
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
    """The SQL builder must emit TiDB FTS_MATCH_WORD, never MySQL
    MATCH..AGAINST, and keep placeholder count == parameter count."""

    def _placeholder_count(self, sql):
        return sql.count("%s")

    # --- FTS_MATCH_WORD, never AGAINST -------------------------------------

    def test_uses_fts_match_word_not_against(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec",
            ["root_cause", "resolution"], [],
        )
        assert "FTS_MATCH_WORD" in sql
        assert "AGAINST" not in sql
        assert "MATCH(" not in sql

    def test_ft_score_alias_present(self):
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", ["root_cause"], [],
        )
        assert "AS ft_score" in sql

    # --- single vs multi column -------------------------------------------

    def test_single_column_is_plain_fts_match_word(self):
        expr = _build_ft_expr(["fm.content"])
        assert expr == "FTS_MATCH_WORD(%s, fm.content)"
        assert "GREATEST" not in expr

    def test_multi_column_uses_greatest(self):
        expr = _build_ft_expr(["root_cause", "resolution"])
        assert expr.startswith("GREATEST(")
        assert expr.count("FTS_MATCH_WORD") == 2
        assert "FTS_MATCH_WORD(%s, root_cause)" in expr
        assert "FTS_MATCH_WORD(%s, resolution)" in expr

    # --- placeholder / parameter count parity -----------------------------

    def test_hybrid_placeholder_count_single_column(self):
        # 1 vec (SELECT distance) + 1 ft (SELECT) + 0 where
        #  + 1 vec (ORDER BY) + 1 ft (ORDER BY) + 1 limit = 5
        sql = _build_hybrid_sql(
            "fleet_memory fm", "fm.memory_vec", ["fm.content"], [],
        )
        assert self._placeholder_count(sql) == 5

    def test_hybrid_placeholder_count_multi_column(self):
        # 1 vec + 2 ft + 0 where + 1 vec + 2 ft + 1 limit = 7
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec",
            ["root_cause", "resolution"], [],
        )
        assert self._placeholder_count(sql) == 7

    def test_hybrid_placeholder_count_with_where(self):
        # multi-column (7) + 2 where placeholders = 9
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec",
            ["root_cause", "resolution"],
            ["severity = %s", "category = %s"],
        )
        assert self._placeholder_count(sql) == 9

    def test_hybrid_param_list_matches_placeholders(self):
        # Reproduce the exact param assembly from _hybrid_search and
        # assert it matches the number of %s in the built SQL.
        ft_columns = ["root_cause", "resolution"]
        where_clauses = ["severity = %s"]
        params = ["safety"]
        sql = _build_hybrid_sql(
            "outage_catalog", "signature_vec", ft_columns, where_clauses,
        )
        n = len(ft_columns)
        query_vec, keyword_str, limit = "[0.1]", "E-001 GroundFailure", 5
        all_params = (
            [query_vec] + [keyword_str] * n + params
            + [query_vec] + [keyword_str] * n + [limit]
        )
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
                ["root_cause"], [],
            )

    def test_disallowed_vec_column_raises(self):
        with pytest.raises(ValueError):
            _build_hybrid_sql(
                "outage_catalog", "signature_vec) --", ["root_cause"], [],
            )

    def test_disallowed_ft_column_raises(self):
        with pytest.raises(ValueError):
            _build_hybrid_sql(
                "outage_catalog", "signature_vec",
                ["root_cause", "pattern_name"], [],  # pattern_name has no FT index
            )

    def test_validate_identifiers_accepts_known(self):
        # Should not raise for the real call-site identifiers.
        _validate_identifiers("outage_catalog", "signature_vec",
                              ["root_cause", "resolution"])
        _validate_identifiers("fleet_memory fm", "fm.memory_vec",
                              ["fm.content"])

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
            ["root_cause", "resolution"], "[0.1]",
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
            ["root_cause", "resolution"], "[0.1]",
            "charger showing E-001 fault", [], [], limit=5,
        )
        assert ft_used is False
        # Two executes: failed hybrid, then vector fallback
        assert len(cur.calls) == 2
        fb_sql, fb_params = cur.calls[1]
        assert "FTS_MATCH_WORD" not in fb_sql
        assert len(fb_params) == fb_sql.count("%s")

    def test_no_keywords_skips_fulltext(self):
        cur = self._FakeCursor(raise_on_hybrid=False)
        rows, ft_used = _hybrid_search(
            cur, "outage_catalog", "signature_vec",
            ["root_cause"], "[0.1]",
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
        assert derive_confidence("verified", 0, 0) == pytest.approx(0.86, abs=0.01)

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
        base = {
            "id": 1,
            "confidence": 0.90,
            "similarity": 0.70,
            "confirmations": 5,
            "contradictions": 0,
            "provenance": "consolidated",
            "superseded_by": None,
        }
        base.update(overrides)
        return base

    def test_high_conf_high_confirmations_accepted(self):
        assert _shortcut_eligible(self._match()) is True

    def test_high_conf_low_confirmations_rejected(self):
        # Confidence passes but only 1 confirmation and not verified.
        m = self._match(confirmations=1, provenance="consolidated")
        assert _shortcut_eligible(m) is False

    def test_verified_provenance_low_confirmations_accepted(self):
        # Verified provenance satisfies the corroboration clause on its own.
        m = self._match(confirmations=0, provenance="verified")
        assert _shortcut_eligible(m) is True

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
