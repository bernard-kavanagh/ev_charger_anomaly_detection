"""
Telemetry Validation Module
=============================
Validates and cleans raw OCPP telemetry before it enters the data plane.
Used by both stream_telemetry.py (--direct mode) and the Flink job.

Design principle: reject clearly impossible values, clamp edge cases,
and flag suspicious-but-plausible readings for downstream attention.
Never silently drop a row — always return it with a validation status.
"""

from dataclasses import dataclass, field
from typing import Optional
import logging

log = logging.getLogger("validation")


@dataclass
class ValidationResult:
    """Result of validating a single telemetry message."""
    is_valid: bool = True
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    cleaned: dict = field(default_factory=dict)

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

# Physical impossibilities (reject)
MAX_VOLTAGE_V = 500.0       # No charger operates above 500V AC
MIN_VOLTAGE_V = 0.0
MAX_POWER_W = 500000         # 500kW is current max for DC fast chargers
MAX_CURRENT_A = 1000.0
MAX_TEMP_C = 120.0           # PCB components fail well before this
MIN_TEMP_C = -40.0           # Operating range for outdoor chargers
MAX_EARTH_LEAK_MA = 50.0     # RCD trips at 30mA; 50mA is physically dangerous
MAX_FAN_RPM = 10000
MAX_CONTACTOR_CYCLES = 500000  # Mechanical lifetime

# Suspicious but plausible (warn)
WARN_TEMP_C = 80.0           # Very hot but not impossible
WARN_EARTH_LEAK_MA = 10.0    # Above RCD threshold
WARN_VOLTAGE_LOW = 200.0     # Severe brownout
WARN_POWER_ZERO_CHARGING = True  # Power=0 while status=Charging


def validate_telemetry(msg: dict) -> ValidationResult:
    """
    Validate a single telemetry message.

    Returns a ValidationResult with:
    - is_valid: False if the message has physically impossible values
    - warnings: list of suspicious-but-plausible issues
    - errors: list of impossible values that caused rejection
    - cleaned: the message with clamped/defaulted values

    Usage:
        result = validate_telemetry(msg)
        if result.is_valid:
            insert(result.cleaned)
        else:
            log.warning(f"Rejected: {result.errors}")
    """
    result = ValidationResult(cleaned=msg.copy())
    c = result.cleaned

    # --- Required fields ---
    if not c.get("charger_id"):
        result.errors.append("missing charger_id")
        result.is_valid = False
        return result

    if not c.get("ts"):
        result.errors.append("missing timestamp")
        result.is_valid = False
        return result

    if not c.get("status"):
        result.errors.append("missing status")
        result.is_valid = False
        return result

    # --- Voltage ---
    voltage = c.get("voltage_v")
    if voltage is not None:
        voltage = float(voltage)
        if voltage < MIN_VOLTAGE_V or voltage > MAX_VOLTAGE_V:
            result.errors.append(f"voltage_v={voltage} outside [{MIN_VOLTAGE_V}, {MAX_VOLTAGE_V}]")
            result.is_valid = False
        elif voltage < WARN_VOLTAGE_LOW and voltage > 0:
            result.warnings.append(f"voltage_v={voltage} below {WARN_VOLTAGE_LOW}V (severe brownout)")

    # --- Power ---
    power = c.get("power_w")
    if power is not None:
        power = int(power)
        if power < 0:
            result.errors.append(f"power_w={power} is negative")
            result.is_valid = False
        elif power > MAX_POWER_W:
            result.errors.append(f"power_w={power} exceeds {MAX_POWER_W}W")
            result.is_valid = False

    # Power=0 while Charging is suspicious
    if c.get("status") == "Charging" and power is not None and power == 0:
        result.warnings.append("power_w=0 during Charging status (possible contactor fault)")

    # --- Current ---
    current = c.get("current_a")
    if current is not None:
        current = float(current)
        if current < 0:
            result.errors.append(f"current_a={current} is negative")
            result.is_valid = False
        elif current > MAX_CURRENT_A:
            result.errors.append(f"current_a={current} exceeds {MAX_CURRENT_A}A")
            result.is_valid = False

    # --- SoC ---
    soc = c.get("soc_percent")
    if soc is not None:
        soc = int(soc)
        if soc < 0 or soc > 100:
            result.warnings.append(f"soc_percent={soc} outside [0, 100], clamping")
            c["soc_percent"] = max(0, min(100, soc))

    # --- Temperature ---
    temp = c.get("temp_c")
    if temp is not None:
        temp = float(temp)
        if temp < MIN_TEMP_C or temp > MAX_TEMP_C:
            result.errors.append(f"temp_c={temp} outside [{MIN_TEMP_C}, {MAX_TEMP_C}]")
            result.is_valid = False
        elif temp > WARN_TEMP_C:
            result.warnings.append(f"temp_c={temp} above {WARN_TEMP_C}C (extreme heat)")

    # --- Earth leakage ---
    leak = c.get("earth_leak_ma")
    if leak is not None:
        leak = float(leak)
        if leak < 0:
            result.warnings.append(f"earth_leak_ma={leak} is negative, clamping to 0")
            c["earth_leak_ma"] = 0.0
        elif leak > MAX_EARTH_LEAK_MA:
            result.errors.append(f"earth_leak_ma={leak} exceeds {MAX_EARTH_LEAK_MA}mA")
            result.is_valid = False
        elif leak > WARN_EARTH_LEAK_MA:
            result.warnings.append(f"earth_leak_ma={leak} above RCD threshold")

    # --- Fan RPM ---
    fan = c.get("fan_rpm")
    if fan is not None:
        fan = int(fan)
        if fan < 0:
            result.warnings.append(f"fan_rpm={fan} is negative, clamping to 0")
            c["fan_rpm"] = 0
        elif fan > MAX_FAN_RPM:
            result.errors.append(f"fan_rpm={fan} exceeds {MAX_FAN_RPM}")
            result.is_valid = False

    # --- Contactor cycles ---
    cycles = c.get("contactor_cycles")
    if cycles is not None:
        cycles = int(cycles)
        if cycles < 0:
            result.warnings.append(f"contactor_cycles={cycles} is negative, clamping to 0")
            c["contactor_cycles"] = 0
        elif cycles > MAX_CONTACTOR_CYCLES:
            result.warnings.append(f"contactor_cycles={cycles} unusually high")

    # --- Error code default ---
    if not c.get("error_code"):
        c["error_code"] = "NoError"

    return result


def validate_window(window: dict) -> ValidationResult:
    """
    Validate a window aggregate row before insert.
    Lighter checks since values are already aggregated.
    """
    result = ValidationResult(cleaned=window.copy())
    c = result.cleaned

    if not c.get("charger_id"):
        result.errors.append("missing charger_id")
        result.is_valid = False

    if not c.get("window_start") or not c.get("window_end"):
        result.errors.append("missing window_start or window_end")
        result.is_valid = False

    # Anomaly score must be [0, 1]
    score = c.get("anomaly_score")
    if score is not None:
        score = float(score)
        if score < 0 or score > 1:
            result.warnings.append(f"anomaly_score={score} outside [0,1], clamping")
            c["anomaly_score"] = max(0.0, min(1.0, score))

    return result
