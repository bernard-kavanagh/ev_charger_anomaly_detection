"""
Shared Text Banding Module
============================
Converts raw numeric telemetry into descriptive natural language phrases
for embedding. Used by BOTH embedding_service.py and stream_telemetry.py
to ensure the vocabulary is identical at ingest and search time.

The key insight: embedding models match "slightly elevated earth leakage"
against "earth leakage sensor drift" far better than "earth_leak=5.2mA"
against the same description. Banding bridges the vocabulary gap between
raw metrics and the outage catalog's symptom descriptions.
"""

from typing import Optional


# ---------------------------------------------------------------------------
# Banding thresholds (domain knowledge)
# ---------------------------------------------------------------------------
# Each banding function maps a numeric value to a descriptive phrase.
# These thresholds are calibrated against the outage_catalog symptom
# descriptions so vector recall is maximised.

def band_power(avg_w: float, max_w: float, max_power_w: float = 50000) -> str:
    """Band power delivery relative to charger capacity."""
    if avg_w <= 0:
        return "no power delivery"
    ratio = avg_w / max_power_w if max_power_w > 0 else 0
    if ratio < 0.1:
        return "minimal power delivery, likely idle or fault state"
    elif ratio < 0.4:
        return "low power output, possible de-rating or session taper"
    elif ratio < 0.7:
        return "moderate power delivery"
    elif ratio < 0.9:
        return "high power delivery, normal fast charge"
    else:
        return "near-maximum power output"


def band_voltage(min_v: float, max_v: float, stddev: float) -> str:
    """Band voltage stability."""
    parts = []
    if min_v < 210:
        parts.append("voltage sag below 210V detected, possible transformer undersizing")
    elif min_v < 220:
        parts.append("slight voltage dip under load")

    if stddev > 12:
        parts.append("severe voltage instability, stddev above 12V")
    elif stddev > 8:
        parts.append("high voltage variance, stddev above 8V")
    elif stddev > 5:
        parts.append("moderate voltage fluctuation")
    else:
        parts.append("stable voltage")

    spread = max_v - min_v
    if spread > 20:
        parts.append(f"wide voltage range of {spread:.0f}V")

    return ". ".join(parts)


def band_temperature(max_c: float, avg_c: float) -> str:
    """Band thermal state."""
    if max_c > 70:
        return "critical temperature above 70C, thermal runaway risk"
    elif max_c > 60:
        return "high temperature above 60C, thermal de-rating likely"
    elif max_c > 50:
        return "elevated temperature, approaching thermal limits"
    elif max_c > 35:
        return "normal operating temperature"
    else:
        return "cool operating temperature"


def band_earth_leak(leak_ma: float) -> str:
    """Band earth leakage current."""
    if leak_ma > 8:
        return "dangerous earth leakage above 8mA, ground failure imminent"
    elif leak_ma > 6:
        return "elevated earth leakage above 6mA, sensor drift or seal failure"
    elif leak_ma > 4:
        return "slightly elevated earth leakage"
    elif leak_ma > 2:
        return "normal earth leakage"
    else:
        return "minimal earth leakage"


def band_fan(rpm: int, power_w: float) -> str:
    """Band cooling fan health."""
    if rpm == 0 and power_w > 0:
        return "fan not spinning during active charging, likely fan failure"
    elif rpm < 500 and power_w > 1000:
        return "fan RPM critically low under load, bearing failure suspected"
    elif rpm < 1000 and power_w > 5000:
        return "fan RPM below expected for load, possible degradation"
    elif rpm > 0:
        return "fan operating normally"
    else:
        return "fan idle, no load"


def band_errors(error_count: int, distinct_errors: Optional[str] = None) -> str:
    """Band error frequency and diversity."""
    if error_count == 0:
        return "no errors"
    elif error_count <= 2:
        base = "occasional errors"
    elif error_count <= 5:
        base = "frequent errors"
    else:
        base = "error storm, multiple fault codes firing"

    if distinct_errors and distinct_errors not in ("null", "[]", ""):
        base += f", error codes: {distinct_errors}"
    return base


def band_status_changes(changes: int) -> str:
    """Band status flapping."""
    if changes <= 2:
        return "stable status"
    elif changes <= 5:
        return "some status transitions"
    elif changes <= 10:
        return "frequent status changes, possible flapping"
    else:
        return "severe status flapping, rapid state transitions"


def band_connectivity(signal_dbm: Optional[int] = None) -> Optional[str]:
    """Band network signal quality."""
    if signal_dbm is None:
        return None
    if signal_dbm < -90:
        return "very weak signal, connectivity issues likely"
    elif signal_dbm < -80:
        return "weak signal"
    elif signal_dbm < -70:
        return "moderate signal"
    else:
        return "strong signal"


# ---------------------------------------------------------------------------
# Anomaly score breakdown (explainability)
# ---------------------------------------------------------------------------

def compute_anomaly_breakdown(row: dict) -> dict:
    """
    Compute per-feature anomaly scores. Returns a dict like:
    {"voltage_instability": 0.18, "thermal_stress": 0.12, ...}

    Each feature score is 0.0-1.0. The overall anomaly_score is their
    weighted sum (capped at 1.0). This replaces the opaque single score
    with an explainable breakdown.
    """
    breakdown = {}

    # Voltage instability (weight: 0.25)
    stddev = float(row.get("voltage_stddev", 0) or 0)
    if stddev > 5.0:
        breakdown["voltage_instability"] = round(min(1.0, (stddev - 5.0) / 10.0), 3)

    # Thermal stress (weight: 0.20)
    max_temp = float(row.get("max_temp_c", 0) or 0)
    if max_temp > 50.0:
        breakdown["thermal_stress"] = round(min(1.0, (max_temp - 50.0) / 30.0), 3)

    # Error rate (weight: 0.25)
    errors = int(row.get("error_count", 0) or 0)
    if errors > 0:
        breakdown["error_rate"] = round(min(1.0, errors / 5.0), 3)

    # Earth leakage (weight: 0.20)
    leak = float(row.get("max_earth_leak", 0) or 0)
    if leak > 4.0:
        breakdown["earth_leakage"] = round(min(1.0, (leak - 4.0) / 8.0), 3)

    # Status flapping (weight: 0.10)
    changes = int(row.get("status_changes", 0) or 0)
    if changes > 5:
        breakdown["status_flapping"] = round(min(1.0, (changes - 5) / 10.0), 3)

    return breakdown


def compute_anomaly_score(breakdown: dict) -> float:
    """Compute weighted anomaly score from breakdown."""
    weights = {
        "voltage_instability": 0.25,
        "thermal_stress": 0.20,
        "error_rate": 0.25,
        "earth_leakage": 0.20,
        "status_flapping": 0.10,
    }
    score = sum(breakdown.get(k, 0) * w for k, w in weights.items())
    return round(min(1.0, score), 3)


# ---------------------------------------------------------------------------
# Main text builder: window → embeddable text
# ---------------------------------------------------------------------------

def build_window_text(row: dict) -> str:
    """
    Convert a charger_windows row into descriptive natural language
    for embedding. This is the SINGLE SOURCE OF TRUTH for how windows
    are described — used by both embedding_service.py and stream_telemetry.py.
    """
    parts = [
        f"Charger {row.get('charger_id', 'unknown')}",
        f"window {row.get('window_start', '')} to {row.get('window_end', '')}",
    ]

    # Power banding
    avg_power = float(row.get("avg_power_w", 0) or 0)
    max_power = float(row.get("max_power_w", 0) or 0)
    parts.append(band_power(avg_power, max_power))

    # Voltage banding
    min_v = float(row.get("min_voltage_v", 0) or 0)
    max_v = float(row.get("max_voltage_v", 0) or 0)
    stddev = float(row.get("voltage_stddev", 0) or 0)
    parts.append(band_voltage(min_v, max_v, stddev))

    # Temperature banding
    max_temp = float(row.get("max_temp_c", 0) or 0)
    avg_temp = float(row.get("avg_temp_c", 0) or 0)
    parts.append(band_temperature(max_temp, avg_temp))

    # Earth leakage banding
    leak = float(row.get("max_earth_leak", 0) or 0)
    parts.append(band_earth_leak(leak))

    # Fan banding
    fan_rpm = int(row.get("avg_fan_rpm", 0) or 0)
    parts.append(band_fan(fan_rpm, avg_power))

    # Error banding
    errors = int(row.get("error_count", 0) or 0)
    distinct = row.get("distinct_errors")
    if isinstance(distinct, list):
        distinct = ", ".join(str(e) for e in distinct)
    parts.append(band_errors(errors, distinct))

    # Status changes
    changes = int(row.get("status_changes", 0) or 0)
    parts.append(band_status_changes(changes))

    # Anomaly score
    score = float(row.get("anomaly_score", 0) or 0)
    if score > 0:
        parts.append(f"anomaly_score={score}")

    # Anomaly flags (keep original for backward compat)
    if row.get("anomaly_flags"):
        parts.append(f"flags={row['anomaly_flags']}")

    return ". ".join(parts)


def build_outage_text(row: dict) -> str:
    """Convert an outage_catalog row into text for embedding."""
    parts = [
        f"Pattern: {row.get('pattern_name', '')}",
        f"Category: {row.get('category', '')}",
        f"Severity: {row.get('severity', '')}",
        f"Root cause: {row.get('root_cause', '')}",
        f"Symptoms: {row.get('symptoms', '')}",
        f"Resolution: {row.get('resolution', '')}",
    ]
    return ". ".join(parts)


def build_reasoning_text(row: dict) -> str:
    """Convert an agent_reasoning row into text for embedding."""
    text = row.get("observation", "")
    if row.get("hypothesis"):
        text += f" | Hypothesis: {row['hypothesis']}"
    if row.get("tags"):
        text += f" | Tags: {row['tags']}"
    return text


def build_memory_text(row: dict) -> str:
    """Convert a fleet_memory row into text for embedding."""
    return f"[{row.get('category', '')}] [{row.get('scope', '')}] {row.get('content', '')}"


TEXT_BUILDERS = {
    "charger_windows": build_window_text,
    "outage_catalog": build_outage_text,
    "agent_reasoning": build_reasoning_text,
    "fleet_memory": build_memory_text,
}
