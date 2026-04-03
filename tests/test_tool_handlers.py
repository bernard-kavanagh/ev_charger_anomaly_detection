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
