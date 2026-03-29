"""
OCPP Telemetry Stream Simulator
=================================
Generates a continuous stream of realistic OCPP-style telemetry messages
for 20,000 chargers. Outputs JSON to stdout (pipe to Kafka, file, etc).

Simulates:
  - Normal charging sessions with realistic power curves
  - Degradation patterns (fan wear, voltage sag, earth leakage drift)
  - Outage events (contactor failure, firmware bugs, network loss)
  - Environmental effects (temperature, coastal corrosion, condensation)
  - Time-of-day usage patterns (peak hours, overnight lulls)

Usage:
    # Stream to stdout (default: 1x realtime)
    python stream_telemetry.py

    # Accelerated for seeding (100x realtime, 1 hour of data)
    python stream_telemetry.py --speed 100 --duration 3600

    # Write to file for bulk loading
    python stream_telemetry.py --speed 1000 --duration 86400 --format csv > telemetry_day.csv

    # Output as Kafka-ready JSON lines
    python stream_telemetry.py --speed 50 --duration 7200 --format jsonl > telemetry.jsonl
"""

import argparse
import csv
import io
import json
import math
import os
import random
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Charger state machine
# ---------------------------------------------------------------------------

class ChargerState:
    """Simulates a single EV charge point's behaviour over time."""

    STATUSES = ["Available", "Preparing", "Charging", "SuspendedEV",
                "SuspendedEVSE", "Finishing", "Faulted", "Unavailable"]

    def __init__(self, charger_id: str, site_id: str, max_power_kw: float,
                 connector_count: int, environment: str,
                 firmware: str, manufacturer: str, model: str,
                 install_days_ago: int, contactor_cycles: int):
        self.charger_id = charger_id
        self.site_id = site_id
        self.max_power_w = int(max_power_kw * 1000)
        self.connector_count = connector_count
        self.environment = environment
        self.firmware = firmware
        self.manufacturer = manufacturer
        self.model = model

        # State
        self.status = "Available"
        self.transaction_id = None
        self.session_energy_wh = 0
        self.session_start = None
        self.target_soc = random.randint(80, 100)
        self.current_soc = 0
        self.vehicle_battery_kwh = random.choice([40, 50, 60, 75, 82, 100])

        # Hardware state
        self.contactor_cycles = contactor_cycles
        self.fan_rpm_baseline = random.randint(2200, 2800)
        self.fan_degradation = 0.0  # 0.0 = new, 1.0 = failed
        self.earth_leak_baseline = random.uniform(0.5, 2.0)
        self.earth_leak_drift = 0.0
        self.cable_resistance_factor = 1.0  # 1.0 = new, increases over time
        self.firmware_bug_active = False
        self.is_offline = False
        self.install_days_ago = install_days_ago

        # Degradation injection
        self._inject_degradation_profile()

    def _inject_degradation_profile(self):
        """Assign degradation profiles to ~5% of chargers for realistic anomalies."""
        r = random.random()
        if r < 0.01:
            # 1%: Severe fan degradation
            self.fan_degradation = random.uniform(0.4, 0.8)
        elif r < 0.025:
            # 1.5%: Earth leakage drift (coastal)
            if self.environment in ("coastal", "outdoor_exposed"):
                self.earth_leak_drift = random.uniform(2.0, 6.0)
        elif r < 0.04:
            # 1.5%: Cable resistance
            self.cable_resistance_factor = random.uniform(1.1, 1.4)
        elif r < 0.05:
            # 1%: Firmware bug (heartbeat flood or SoC stuck)
            if self.firmware in ("ABB 3.1.2", "Wallbox 5.2.1", "EVBox 4.0.3"):
                self.firmware_bug_active = True

    def tick(self, now: datetime, hour_of_day: float) -> Optional[dict]:
        """Advance state by one tick (15 seconds). Returns telemetry or None."""
        if self.is_offline:
            if random.random() < 0.001:  # ~0.1% chance of coming back per tick
                self.is_offline = False
                self.status = "Available"
            else:
                return None

        # Usage probability based on time of day
        usage_prob = self._usage_probability(hour_of_day)

        if self.status == "Available":
            if random.random() < usage_prob * 0.003:
                self._start_session(now)

        elif self.status == "Charging":
            self._advance_charging(now)

            # Random chance of session end
            if self.current_soc >= self.target_soc or random.random() < 0.002:
                self._end_session(now)

        elif self.status == "Finishing":
            if random.random() < 0.3:
                self.status = "Available"
                self.transaction_id = None

        # Random faults (~0.02% per tick)
        if random.random() < 0.0002 and self.status not in ("Faulted", "Unavailable"):
            self.status = "Faulted"

        if self.status == "Faulted" and random.random() < 0.01:
            self.status = "Available"

        return self._build_telemetry(now, hour_of_day)

    def _usage_probability(self, hour: float) -> float:
        """Time-of-day usage curve (peaks at 8am, 12pm, 6pm)."""
        morning = math.exp(-((hour - 8) ** 2) / 8)
        lunch = math.exp(-((hour - 12.5) ** 2) / 6)
        evening = math.exp(-((hour - 18) ** 2) / 10)
        overnight = 0.05
        return max(overnight, morning * 0.8 + lunch * 0.5 + evening * 1.0)

    def _start_session(self, now: datetime):
        self.status = "Charging"
        self.transaction_id = f"txn-{random.randint(100000, 999999):06x}"
        self.session_energy_wh = 0
        self.session_start = now
        self.current_soc = random.randint(10, 60)
        self.target_soc = random.randint(80, 100)
        self.vehicle_battery_kwh = random.choice([40, 50, 60, 75, 82, 100])
        self.contactor_cycles += 1

    def _advance_charging(self, now: datetime):
        """Simulate 15 seconds of charging with realistic power curve."""
        # Power curve: high at low SoC, tapers above 80%
        soc_factor = 1.0 if self.current_soc < 80 else (1.0 - (self.current_soc - 80) / 40)
        soc_factor = max(0.1, soc_factor)

        power_w = int(self.max_power_w * soc_factor * random.uniform(0.85, 0.98))

        # Apply degradation
        power_w = int(power_w / self.cable_resistance_factor)
        if self.fan_degradation > 0.3:
            thermal_derate = max(0.5, 1.0 - self.fan_degradation * 0.6)
            power_w = int(power_w * thermal_derate)

        energy_increment = power_w * 15 / 3600  # Wh for 15 seconds
        self.session_energy_wh += energy_increment
        self.current_soc = min(100, self.current_soc +
                               energy_increment / (self.vehicle_battery_kwh * 10))

    def _end_session(self, now: datetime):
        self.status = "Finishing"
        self.contactor_cycles += 1

    def _build_telemetry(self, now: datetime, hour: float) -> dict:
        """Build a telemetry message with all sensor readings."""
        # Base voltage (site-dependent with time-of-day sag)
        peak_sag = 1.0 if hour < 7 or hour > 22 else 0.97
        base_voltage = random.gauss(230.0 * peak_sag, 2.0)

        # Power & current
        if self.status == "Charging":
            soc_factor = 1.0 if self.current_soc < 80 else max(0.1, 1.0 - (self.current_soc - 80) / 40)
            power_w = int(self.max_power_w * soc_factor * random.uniform(0.85, 0.98))
            power_w = int(power_w / self.cable_resistance_factor)
            current_a = round(power_w / base_voltage, 1) if base_voltage > 0 else 0
        else:
            power_w = 0
            current_a = 0.0

        # Temperature
        ambient = 8 + 7 * math.sin((hour - 6) * math.pi / 12)  # Irish temps 1-15C
        load_heat = (power_w / self.max_power_w * 25) if self.max_power_w > 0 else 0
        fan_penalty = self.fan_degradation * 15
        temp_c = round(ambient + load_heat + fan_penalty + random.gauss(0, 1.5), 1)

        # Fan RPM
        target_rpm = self.fan_rpm_baseline if power_w > 0 else int(self.fan_rpm_baseline * 0.3)
        fan_rpm = max(0, int(target_rpm * (1.0 - self.fan_degradation) + random.gauss(0, 50)))

        # Earth leakage
        earth_leak = round(self.earth_leak_baseline + self.earth_leak_drift +
                           random.gauss(0, 0.3), 1)
        earth_leak = max(0.0, earth_leak)

        # Error code
        error_code = "NoError"
        if earth_leak > 8.0:
            error_code = "GroundFailure"
        elif temp_c > 65:
            error_code = "HighTemperature"
        elif self.status == "Faulted":
            error_code = random.choice(["InternalError", "OtherError",
                                         "PowerMeterFailure", "GroundFailure"])

        # Signal strength
        signal_dbm = random.gauss(-65, 8)
        if self.environment == "indoor":
            signal_dbm -= 10

        return {
            "charger_id": self.charger_id,
            "connector_id": 1,
            "ts": now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
            "status": self.status,
            "transaction_id": self.transaction_id,
            "energy_wh": int(self.session_energy_wh),
            "power_w": power_w,
            "voltage_v": round(base_voltage, 2),
            "current_a": round(current_a, 1),
            "soc_percent": int(self.current_soc) if self.status == "Charging" else None,
            "temp_c": temp_c,
            "error_code": error_code,
            "vendor_error": None,
            "fan_rpm": fan_rpm,
            "earth_leak_ma": earth_leak,
            "contactor_cycles": self.contactor_cycles,
            "signal_dbm": int(signal_dbm),
        }


# ---------------------------------------------------------------------------
# Fleet simulator
# ---------------------------------------------------------------------------

class FleetSimulator:
    """Manages the state of all 20,000 chargers."""

    def __init__(self, registry_file: str = None, charger_count: int = 200):
        """
        If registry_file is provided, load charger configs from it.
        Otherwise generate a small fleet for testing.
        """
        self.chargers = []

        if registry_file:
            self._load_from_registry(registry_file)
        else:
            self._generate_test_fleet(charger_count)

    def _generate_test_fleet(self, count: int):
        """Generate a small test fleet."""
        envs = ["indoor", "outdoor_sheltered", "outdoor_exposed", "coastal"]
        mfrs = [
            ("ABB", "Terra 54", 50.0, "3.1.2"),
            ("ABB", "Terra 124", 120.0, "3.2.1"),
            ("Tritium", "RT50", 50.0, "2.5.0"),
            ("EVBox", "Troniq 50", 50.0, "4.0.3"),
            ("Wallbox", "Supernova 65", 65.0, "5.3.0"),
            ("Kempower", "T-Series 200", 200.0, "1.4.0"),
            ("Alpitronic", "Hypercharger 150", 150.0, "1.10.0"),
        ]

        for i in range(count):
            mfr, model, kw, fw = random.choice(mfrs)
            self.chargers.append(ChargerState(
                charger_id=f"CP-IE-TEST-{i+1:05d}",
                site_id=f"SITE-IE-TEST-{(i // 10) + 1:03d}",
                max_power_kw=kw,
                connector_count=random.choice([1, 2]),
                environment=random.choice(envs),
                firmware=fw,
                manufacturer=mfr,
                model=model,
                install_days_ago=random.randint(180, 1825),
                contactor_cycles=random.randint(1000, 50000),
            ))

    def _load_from_registry(self, path: str):
        """Load charger configs from the seed registry JSON."""
        with open(path) as f:
            registry = json.load(f)

        for c in registry:
            self.chargers.append(ChargerState(
                charger_id=c["charger_id"],
                site_id=c["site_id"],
                max_power_kw=c["max_power_kw"],
                connector_count=c["connector_count"],
                environment=c["environment"],
                firmware=c["firmware_version"],
                manufacturer=c["manufacturer"],
                model=c["model"],
                install_days_ago=random.randint(180, 1825),
                contactor_cycles=random.randint(1000, 50000),
            ))

    def tick(self, now: datetime) -> list[dict]:
        """Advance all chargers by one 15-second tick. Returns telemetry list."""
        hour = now.hour + now.minute / 60.0
        messages = []
        for charger in self.chargers:
            msg = charger.tick(now, hour)
            if msg:
                messages.append(msg)
        return messages


# ---------------------------------------------------------------------------
# Output formatters
# ---------------------------------------------------------------------------

def output_jsonl(messages: list[dict]):
    for msg in messages:
        print(json.dumps(msg, default=str))


def output_csv_header():
    fields = ["charger_id", "connector_id", "ts", "status", "transaction_id",
              "energy_wh", "power_w", "voltage_v", "current_a", "soc_percent",
              "temp_c", "error_code", "vendor_error", "fan_rpm",
              "earth_leak_ma", "contactor_cycles", "signal_dbm"]
    print(",".join(fields))


def output_csv(messages: list[dict]):
    for msg in messages:
        values = [str(msg.get(f, "")) for f in [
            "charger_id", "connector_id", "ts", "status", "transaction_id",
            "energy_wh", "power_w", "voltage_v", "current_a", "soc_percent",
            "temp_c", "error_code", "vendor_error", "fan_rpm",
            "earth_leak_ma", "contactor_cycles", "signal_dbm"
        ]]
        print(",".join(values))


def output_sql(messages: list[dict]):
    if not messages:
        return
    batch_size = 200
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        print("INSERT INTO charger_telemetry")
        print("  (charger_id, connector_id, ts, status, transaction_id,")
        print("   energy_wh, power_w, voltage_v, current_a, soc_percent,")
        print("   temp_c, error_code, vendor_error, fan_rpm,")
        print("   earth_leak_ma, contactor_cycles, signal_dbm)")
        print("VALUES")
        rows = []
        for m in batch:
            soc = str(m["soc_percent"]) if m["soc_percent"] is not None else "NULL"
            txn = f"'{m['transaction_id']}'" if m["transaction_id"] else "NULL"
            vendor = f"'{m['vendor_error']}'" if m["vendor_error"] else "NULL"
            rows.append(
                f"  ('{m['charger_id']}', {m['connector_id']}, "
                f"'{m['ts']}', '{m['status']}', {txn}, "
                f"{m['energy_wh']}, {m['power_w']}, {m['voltage_v']}, "
                f"{m['current_a']}, {soc}, "
                f"{m['temp_c']}, '{m['error_code']}', {vendor}, "
                f"{m['fan_rpm']}, {m['earth_leak_ma']}, "
                f"{m['contactor_cycles']}, {m['signal_dbm']})"
            )
        print(",\n".join(rows) + ";\n")


# ---------------------------------------------------------------------------
# Direct-to-TiDB mode
# ---------------------------------------------------------------------------

def _window_start(ts: datetime) -> datetime:
    """Floor a timestamp to the nearest 5-minute clock boundary."""
    return ts.replace(second=0, microsecond=0, minute=(ts.minute // 5) * 5)


class WindowAccumulator:
    """Buckets telemetry messages by (charger_id, window_start) and flushes closed windows."""

    def __init__(self):
        # {(charger_id, window_start): [messages]}
        self._buckets: dict = {}

    def add(self, msg: dict, wstart: datetime):
        self._buckets.setdefault((msg['charger_id'], wstart), []).append(msg)

    def flush_closed(self, sim_time: datetime) -> list[dict]:
        """Return computed rows for all windows that ended before sim_time's window."""
        current_wstart = _window_start(sim_time)
        to_flush = [k for k in self._buckets if k[1] < current_wstart]
        rows = []
        for key in to_flush:
            charger_id, wstart = key
            rows.append(_compute_window_row(
                charger_id, wstart,
                wstart + timedelta(minutes=5),
                self._buckets.pop(key),
            ))
        return rows

    def flush_all(self) -> list[dict]:
        """Flush all remaining buckets (end of simulation)."""
        rows = []
        for (charger_id, wstart), messages in list(self._buckets.items()):
            rows.append(_compute_window_row(
                charger_id, wstart,
                wstart + timedelta(minutes=5),
                messages,
            ))
        self._buckets.clear()
        return rows


def _compute_window_row(charger_id: str, wstart: datetime, wend: datetime,
                        messages: list[dict]) -> dict:
    """Aggregate a bucket of messages into a charger_windows row."""
    powers      = [m['power_w']       for m in messages if m.get('power_w')       is not None]
    voltages    = [float(m['voltage_v'])   for m in messages if m.get('voltage_v')   is not None]
    currents    = [float(m['current_a'])   for m in messages if m.get('current_a')   is not None]
    temps       = [float(m['temp_c'])      for m in messages if m.get('temp_c')      is not None]
    fan_rpms    = [m['fan_rpm']        for m in messages if m.get('fan_rpm')        is not None]
    earth_leaks = [float(m['earth_leak_ma']) for m in messages if m.get('earth_leak_ma') is not None]

    avg_power_w    = round(sum(powers) / len(powers), 2)       if powers      else 0.0
    max_power_w    = max(powers)                                if powers      else 0
    min_voltage_v  = round(min(voltages), 2)                   if voltages    else None
    max_voltage_v  = round(max(voltages), 2)                   if voltages    else None
    voltage_stddev = round(statistics.pstdev(voltages), 3)     if voltages    else 0.0
    avg_current_a  = round(sum(currents) / len(currents), 2)   if currents    else 0.0
    max_temp_c     = round(max(temps), 2)                      if temps       else None
    avg_temp_c     = round(sum(temps) / len(temps), 2)         if temps       else None
    avg_fan_rpm    = int(sum(fan_rpms) / len(fan_rpms))        if fan_rpms    else 0
    max_earth_leak = round(max(earth_leaks), 2)                if earth_leaks else 0.0

    error_count   = sum(1 for m in messages if m.get('error_code', 'NoError') != 'NoError')
    status_changes = len({m['status'] for m in messages if m.get('status')})

    error_codes = sorted({
        m['error_code'] for m in messages
        if m.get('error_code') and m['error_code'] != 'NoError'
    })
    distinct_errors = json.dumps(error_codes) if error_codes else None

    # Anomaly flags — first matching rule wins
    if voltage_stddev > 8.0:
        anomaly_flags = json.dumps(["high_voltage_variance"])
    elif max_temp_c is not None and max_temp_c > 60.0:
        anomaly_flags = json.dumps(["high_temperature"])
    elif error_count > 3:
        anomaly_flags = json.dumps(["frequent_errors"])
    elif max_earth_leak > 6.0:
        anomaly_flags = json.dumps(["earth_leakage"])
    elif avg_fan_rpm < 500 and avg_power_w > 1000:
        anomaly_flags = json.dumps(["fan_failure"])
    elif status_changes > 10:
        anomaly_flags = json.dumps(["status_flapping"])
    else:
        anomaly_flags = None

    # Anomaly score — weighted, clamped to [0, 1]
    score = 0.0
    if voltage_stddev > 5.0:
        score += max(0.0, (voltage_stddev - 5.0) / 10.0) * 0.25
    if max_temp_c is not None and max_temp_c > 50.0:
        score += max(0.0, (max_temp_c - 50.0) / 30.0) * 0.20
    if error_count > 0:
        score += min(1.0, error_count / 5.0) * 0.25
    if max_earth_leak > 4.0:
        score += max(0.0, (max_earth_leak - 4.0) / 8.0) * 0.20
    if status_changes > 5:
        score += min(1.0, (status_changes - 5) / 10.0) * 0.10
    anomaly_score = round(min(1.0, score), 3)

    return {
        'charger_id':     charger_id,
        'window_start':   wstart.strftime('%Y-%m-%d %H:%M:%S'),
        'window_end':     wend.strftime('%Y-%m-%d %H:%M:%S'),
        'msg_count':      len(messages),
        'avg_power_w':    avg_power_w,
        'max_power_w':    max_power_w,
        'min_voltage_v':  min_voltage_v,
        'max_voltage_v':  max_voltage_v,
        'voltage_stddev': voltage_stddev,
        'avg_current_a':  avg_current_a,
        'max_temp_c':     max_temp_c,
        'avg_temp_c':     avg_temp_c,
        'error_count':    error_count,
        'status_changes': status_changes,
        'distinct_errors': distinct_errors,
        'avg_fan_rpm':    avg_fan_rpm,
        'max_earth_leak': max_earth_leak,
        'anomaly_flags':  anomaly_flags,
        'anomaly_score':  anomaly_score,
    }


def _get_direct_db():
    import pymysql
    ssl_ca = os.environ.get("TIDB_SSL_CA")
    return pymysql.connect(
        host=os.environ["TIDB_HOST"],
        port=int(os.environ.get("TIDB_PORT", "4000")),
        user=os.environ["TIDB_USER"],
        password=os.environ["TIDB_PASSWORD"],
        database=os.environ["TIDB_DATABASE"],
        ssl={"ca": ssl_ca} if ssl_ca else None,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


_TELEMETRY_INSERT = """
    INSERT INTO charger_telemetry
        (charger_id, connector_id, ts, status, transaction_id,
         energy_wh, power_w, voltage_v, current_a, soc_percent,
         temp_c, error_code, vendor_error, fan_rpm,
         earth_leak_ma, contactor_cycles, signal_dbm)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

_WINDOWS_INSERT = """
    INSERT INTO charger_windows
        (charger_id, window_start, window_end, msg_count,
         avg_power_w, max_power_w, min_voltage_v, max_voltage_v,
         voltage_stddev, avg_current_a, max_temp_c, avg_temp_c,
         error_count, status_changes, distinct_errors, avg_fan_rpm,
         max_earth_leak, anomaly_flags, anomaly_score)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""


def _insert_telemetry_batch(db, messages: list[dict]):
    batch_size = 200
    with db.cursor() as cur:
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i + batch_size]
            rows = [
                (m['charger_id'], m['connector_id'], m['ts'], m['status'],
                 m.get('transaction_id'), m.get('energy_wh'), m.get('power_w'),
                 m.get('voltage_v'), m.get('current_a'), m.get('soc_percent'),
                 m.get('temp_c'), m.get('error_code'), m.get('vendor_error'),
                 m.get('fan_rpm'), m.get('earth_leak_ma'), m.get('contactor_cycles'),
                 m.get('signal_dbm'))
                for m in batch
            ]
            cur.executemany(_TELEMETRY_INSERT, rows)


def _insert_windows_batch(db, windows: list[dict]):
    with db.cursor() as cur:
        rows = [
            (w['charger_id'], w['window_start'], w['window_end'], w['msg_count'],
             w['avg_power_w'], w['max_power_w'], w['min_voltage_v'], w['max_voltage_v'],
             w['voltage_stddev'], w['avg_current_a'], w['max_temp_c'], w['avg_temp_c'],
             w['error_count'], w['status_changes'], w['distinct_errors'], w['avg_fan_rpm'],
             w['max_earth_leak'], w['anomaly_flags'], w['anomaly_score'])
            for w in windows
        ]
        cur.executemany(_WINDOWS_INSERT, rows)


class DirectWriter:
    def __init__(self, db):
        self._db = db
        self._accumulator = WindowAccumulator()
        self._total_telemetry = 0
        self._total_windows = 0
        self._total_anomaly_windows = 0

    def write_tick(self, messages: list[dict], sim_time: datetime):
        if messages:
            _insert_telemetry_batch(self._db, messages)
            self._total_telemetry += len(messages)

        wstart = _window_start(sim_time)
        for msg in messages:
            self._accumulator.add(msg, wstart)

        closed = self._accumulator.flush_closed(sim_time)
        if closed:
            _insert_windows_batch(self._db, closed)
            anomalous = sum(1 for w in closed if w['anomaly_score'] > 0)
            self._total_windows += len(closed)
            self._total_anomaly_windows += anomalous
            print(
                f"-- Flushed {len(closed)} windows ({anomalous} anomalous) | "
                f"telemetry: {self._total_telemetry} rows, "
                f"windows: {self._total_windows} total "
                f"({self._total_anomaly_windows} anomalous)",
                file=sys.stderr,
            )

    def finalize(self):
        remaining = self._accumulator.flush_all()
        if remaining:
            _insert_windows_batch(self._db, remaining)
            anomalous = sum(1 for w in remaining if w['anomaly_score'] > 0)
            self._total_windows += len(remaining)
            self._total_anomaly_windows += anomalous
        print(
            f"-- Complete: {self._total_telemetry} telemetry rows, "
            f"{self._total_windows} windows "
            f"({self._total_anomaly_windows} anomalous)",
            file=sys.stderr,
        )


def _run_direct(fleet: FleetSimulator, args):
    db = _get_direct_db()
    writer = DirectWriter(db)

    sim_time = datetime.now(timezone.utc)
    tick_delta = timedelta(seconds=args.tick_interval)
    real_sleep = args.tick_interval / args.speed if args.speed > 0 else 0
    elapsed = 0

    try:
        while True:
            messages = fleet.tick(sim_time)
            writer.write_tick(messages, sim_time)

            sim_time += tick_delta
            elapsed += args.tick_interval

            if args.duration > 0 and elapsed >= args.duration:
                break

            if real_sleep > 0.01:
                time.sleep(real_sleep)

    except KeyboardInterrupt:
        print(f"\n-- Stopped after {elapsed}s simulated time", file=sys.stderr)
    finally:
        writer.finalize()
        db.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="OCPP Telemetry Stream Simulator")
    parser.add_argument("--speed", type=float, default=1.0,
                        help="Simulation speed multiplier (1=realtime, 100=fast)")
    parser.add_argument("--duration", type=int, default=0,
                        help="Duration in simulated seconds (0=infinite)")
    parser.add_argument("--format", choices=["jsonl", "csv", "sql", "direct"],
                        default="jsonl")
    parser.add_argument("--chargers", type=int, default=200,
                        help="Number of chargers to simulate (default 200 for testing)")
    parser.add_argument("--registry", type=str, default=None,
                        help="Path to registry JSON file for full fleet simulation")
    parser.add_argument("--tick-interval", type=int, default=15,
                        help="Seconds between telemetry messages per charger")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    fleet = FleetSimulator(
        registry_file=args.registry,
        charger_count=args.chargers,
    )

    print(f"-- Simulating {len(fleet.chargers)} chargers at {args.speed}x speed",
          file=sys.stderr)

    if args.format == "direct":
        _run_direct(fleet, args)
        return

    if args.format == "csv":
        output_csv_header()

    sim_time = datetime.now(timezone.utc)
    tick_delta = timedelta(seconds=args.tick_interval)
    real_sleep = args.tick_interval / args.speed if args.speed > 0 else 0
    elapsed = 0

    output_fn = {
        "jsonl": output_jsonl,
        "csv": output_csv,
        "sql": output_sql,
    }[args.format]

    try:
        while True:
            messages = fleet.tick(sim_time)
            output_fn(messages)
            sys.stdout.flush()

            sim_time += tick_delta
            elapsed += args.tick_interval

            if args.duration > 0 and elapsed >= args.duration:
                break

            if real_sleep > 0.01:
                time.sleep(real_sleep)

    except KeyboardInterrupt:
        print(f"\n-- Stopped after {elapsed}s simulated time", file=sys.stderr)
    except BrokenPipeError:
        pass


if __name__ == "__main__":
    main()
