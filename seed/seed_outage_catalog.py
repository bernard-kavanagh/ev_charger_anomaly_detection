"""
Seed generator: outage_catalog
================================
Generates ~50 curated failure patterns that represent the ground truth
for similarity search. These are the patterns the agent will match against.

Usage:
    python seed_outage_catalog.py > seed_outages.sql
"""

import json

PATTERNS = [
    # === ELECTRICAL ===
    {
        "pattern_id": "E-001", "pattern_name": "Contactor weld failure",
        "category": "electrical", "severity": "offline",
        "root_cause": "DC contactor contacts welded shut due to repeated high-current arcing during connector disconnect under load. Common after 40,000+ contactor cycles.",
        "symptoms": ["contactor_cycles > 40000", "status stuck in Charging", "power_w drops to 0 but status does not change", "error_code: GroundFailure or OtherError"],
        "resolution": "Replace DC contactor assembly. Inspect connector pins for arc damage. Update firmware to enforce soft-disconnect before contactor open.",
        "affected_models": ["ABB Terra 54", "ABB Terra 124", "Tritium RT50"],
        "affected_firmware": ["ABB 3.1.x", "Tritium 2.4.x"],
    },
    {
        "pattern_id": "E-002", "pattern_name": "Earth leakage sensor drift",
        "category": "electrical", "severity": "safety",
        "root_cause": "RCD/earth leakage monitoring sensor drifts out of calibration, typically in high-humidity coastal environments. Reports false positives or fails to detect genuine leakage.",
        "symptoms": ["earth_leak_ma fluctuating > 5mA baseline", "intermittent GroundFailure errors", "environment: coastal or outdoor_exposed", "voltage_stddev elevated"],
        "resolution": "Replace earth leakage sensor module. Apply conformal coating to PCB if coastal environment. Recalibrate after replacement.",
        "affected_models": ["ABB Terra 54", "EVBox Troniq 50"],
        "affected_firmware": ["ABB 3.1.2", "EVBox 4.0.3"],
    },
    {
        "pattern_id": "E-003", "pattern_name": "Voltage sag under load",
        "category": "electrical", "severity": "degraded",
        "root_cause": "Site-level transformer undersized for concurrent charging load. Voltage drops below charger minimum during multi-unit peak draw.",
        "symptoms": ["min_voltage_v < 210 during peak hours", "voltage_stddev > 8.0", "multiple chargers at same site affected", "power_w oscillates as chargers compete"],
        "resolution": "Site electrical survey required. Options: upgrade transformer, install load management system, reduce concurrent charging limit via OCPP smart charging profiles.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "E-004", "pattern_name": "Phase imbalance",
        "category": "electrical", "severity": "degraded",
        "root_cause": "Three-phase supply has significant phase imbalance, causing the charger to de-rate or intermittently fault. Often caused by unbalanced single-phase loads on the same distribution board.",
        "symptoms": ["current_a varies > 15% between readings at same power level", "power_w inconsistent at same SoC", "status flips between Charging and SuspendedEVSE"],
        "resolution": "Measure phase voltages at distribution board. Rebalance loads or install phase balancer. Check for loose neutral connections.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "E-005", "pattern_name": "Cable resistance degradation",
        "category": "electrical", "severity": "degraded",
        "root_cause": "Charging cable internal resistance increases over time due to repeated bending, connector pin wear, or water ingress. Causes voltage drop and power de-rating.",
        "symptoms": ["power_w declining trend over weeks at same SoC", "temp_c at connector elevated", "voltage_v lower than expected for site supply", "no error_code but reduced performance"],
        "resolution": "Replace charging cable. Inspect connector housing for water damage. If recurring, investigate cable management (kink protection, drainage).",
        "affected_models": None,
        "affected_firmware": None,
    },

    # === MECHANICAL ===
    {
        "pattern_id": "M-001", "pattern_name": "Cooling fan bearing failure",
        "category": "mechanical", "severity": "degraded",
        "root_cause": "Internal cooling fan bearing wears out, reducing airflow. Charger thermally de-rates to prevent overheating. More common in outdoor units exposed to dust/salt.",
        "symptoms": ["fan_rpm declining trend over weeks", "temp_c rising at same ambient and load", "power_w reduced (thermal de-rating)", "no error_code initially"],
        "resolution": "Replace cooling fan assembly. Clean dust filters. For coastal sites, consider upgrading to IP67-rated fan.",
        "affected_models": ["ABB Terra 124", "ABB Terra 184", "Alpitronic Hypercharger 150"],
        "affected_firmware": None,
    },
    {
        "pattern_id": "M-002", "pattern_name": "Connector latch mechanism failure",
        "category": "mechanical", "severity": "offline",
        "root_cause": "CCS2 or Type 2 connector latch mechanism fails, preventing secure connection. Charger detects incomplete coupling and refuses to start.",
        "symptoms": ["status stuck at Preparing", "rapid status changes Preparing -> Available -> Preparing", "error_code: ConnectorLockFailure", "high session abandonment rate"],
        "resolution": "Replace connector latch assembly. Check for debris in connector housing. Verify actuator solenoid operation.",
        "affected_models": ["EVBox Troniq 50", "EVBox Troniq 100", "Wallbox Supernova 65"],
        "affected_firmware": None,
    },
    {
        "pattern_id": "M-003", "pattern_name": "Enclosure seal failure",
        "category": "mechanical", "severity": "safety",
        "root_cause": "Cabinet door seal or cable gland seal degrades, allowing water ingress. Can cause short circuits, corrosion, and earth leakage faults.",
        "symptoms": ["earth_leak_ma spikes after rain events", "error_code: GroundFailure correlated with weather", "environment: outdoor_exposed or coastal", "intermittent connectivity loss"],
        "resolution": "Replace all cabinet seals and cable glands. Inspect internal components for corrosion. Dry thoroughly before re-energising. Consider IP rating upgrade for exposed sites.",
        "affected_models": None,
        "affected_firmware": None,
    },

    # === THERMAL ===
    {
        "pattern_id": "T-001", "pattern_name": "Thermal de-rating in direct sun",
        "category": "thermal", "severity": "degraded",
        "root_cause": "Charger in direct sunlight exceeds ambient thermal envelope. Internal temperature rises, triggering thermal protection that reduces output power.",
        "symptoms": ["temp_c > 55 during afternoon hours", "power_w reduced by 20-50%", "fan_rpm at maximum", "pattern correlates with solar noon", "no error_code"],
        "resolution": "Install shade canopy. If not possible, apply thermal management profile via OCPP to pre-emptively limit power during peak heat. Consider relocating unit.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "T-002", "pattern_name": "Thermal runaway risk",
        "category": "thermal", "severity": "safety",
        "root_cause": "Internal component (usually power module or DC-DC converter) developing thermal fault. Temperature rises faster than cooling can compensate, even at low load.",
        "symptoms": ["temp_c rising while power_w is low or zero", "temp_c > 70", "fan_rpm at maximum", "error_code: HighTemperature", "no correlation with ambient temperature"],
        "resolution": "IMMEDIATE: De-energise unit. Inspect power electronics for signs of component failure (discolouration, bulging capacitors). Replace affected module. Do not re-energise until inspected.",
        "affected_models": ["Tritium RT50", "Tritium PKM150"],
        "affected_firmware": ["Tritium 2.4.1"],
    },

    # === FIRMWARE ===
    {
        "pattern_id": "F-001", "pattern_name": "OCPP heartbeat timeout loop",
        "category": "firmware", "severity": "offline",
        "root_cause": "Firmware bug causes heartbeat interval to reset to 0 after configuration update, flooding the central system with heartbeats and triggering rate limiting, which then causes the charger to appear offline.",
        "symptoms": ["heartbeat_delta_s < 2 (flooding)", "signal_dbm normal", "charger reachable via network but status shows Unavailable", "follows recent configuration push"],
        "resolution": "Apply firmware patch (fixed in ABB 3.2.0+, EVBox 4.1.0+). Workaround: manually set heartbeat interval via OCPP ChangeConfiguration.",
        "affected_models": ["ABB Terra 54", "ABB Terra 124", "EVBox Troniq 50"],
        "affected_firmware": ["ABB 3.1.2", "ABB 3.1.5", "EVBox 4.0.3"],
    },
    {
        "pattern_id": "F-002", "pattern_name": "SoC reporting stuck",
        "category": "firmware", "severity": "degraded",
        "root_cause": "Firmware bug in meter value reporting causes State of Charge to freeze at a specific value mid-session. Charging continues but SoC-based smart charging decisions are wrong.",
        "symptoms": ["soc_percent constant for 30+ minutes during active Charging", "energy_wh still incrementing", "power_w normal", "affects specific firmware version"],
        "resolution": "Update firmware. Workaround: configure OCPP to use energy-based (Wh) rather than SoC-based session limits.",
        "affected_models": ["Wallbox Pulsar Plus 22", "Wallbox Supernova 65"],
        "affected_firmware": ["Wallbox 5.2.1"],
    },
    {
        "pattern_id": "F-003", "pattern_name": "Transaction ID collision",
        "category": "firmware", "severity": "degraded",
        "root_cause": "Firmware generates duplicate transaction IDs after integer overflow at 65535 sessions. Causes billing errors and session history corruption.",
        "symptoms": ["transaction_id values repeating", "total_sessions approaching 65535", "billing discrepancies reported", "no charger-side errors visible"],
        "resolution": "Update firmware to use 32-bit transaction IDs. Clear transaction counter via OCPP Reset.",
        "affected_models": ["Kempower S-Series 40"],
        "affected_firmware": ["Kempower 1.2.0"],
    },
    {
        "pattern_id": "F-004", "pattern_name": "Meter value reporting gap",
        "category": "firmware", "severity": "degraded",
        "root_cause": "Firmware occasionally skips meter value reports during high-load transitions (ramp-up from 0 to max power). Creates gaps in telemetry that confuse anomaly detection.",
        "symptoms": ["msg_count lower than expected for window", "gaps in power_w time series", "occurs during session start", "no error_code"],
        "resolution": "Update firmware. Reduce MeterValueSampleInterval to 5s to increase reporting frequency and mask occasional gaps.",
        "affected_models": ["Alpitronic Hypercharger 150", "Alpitronic Hypercharger 300"],
        "affected_firmware": ["Alpitronic 1.8.0", "Alpitronic 1.9.2"],
    },

    # === NETWORK ===
    {
        "pattern_id": "N-001", "pattern_name": "Cellular modem degradation",
        "category": "network", "severity": "degraded",
        "root_cause": "4G/LTE modem performance degrades over time, causing intermittent connectivity. Charger continues to operate locally but loses contact with central system.",
        "symptoms": ["signal_dbm declining trend (below -85)", "heartbeat_delta_s increasing", "intermittent status gaps in telemetry", "charger still functions locally"],
        "resolution": "Replace cellular modem or SIM. Check antenna connections. Consider external antenna for poor signal areas. Verify APN configuration.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "N-002", "pattern_name": "WebSocket reconnection storm",
        "category": "network", "severity": "offline",
        "root_cause": "After network outage, all chargers at a site reconnect simultaneously, overwhelming the OCPP gateway. Some chargers fail reconnection and enter a retry loop.",
        "symptoms": ["multiple chargers at same site go Unavailable simultaneously", "signal_dbm normal", "preceded by network outage", "chargers recover at different rates"],
        "resolution": "Configure randomised reconnection backoff in firmware. Increase OCPP gateway connection pool. Add connection rate limiting at load balancer.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "N-003", "pattern_name": "DNS resolution failure",
        "category": "network", "severity": "offline",
        "root_cause": "Charger loses ability to resolve OCPP gateway hostname. Often caused by ISP DNS issues or DHCP lease expiry not renewing DNS settings.",
        "symptoms": ["charger offline but pingable by IP", "no heartbeats", "signal_dbm normal", "preceded by DHCP lease renewal time"],
        "resolution": "Configure secondary DNS server. Use static DNS entries where possible. Monitor DHCP lease expiry times.",
        "affected_models": None,
        "affected_firmware": None,
    },

    # === ENVIRONMENTAL ===
    {
        "pattern_id": "ENV-001", "pattern_name": "Salt spray corrosion (coastal)",
        "category": "environmental", "severity": "degraded",
        "root_cause": "Coastal salt spray causes accelerated corrosion of external connectors, cooling vents, and internal PCB traces. Develops over 12-24 months.",
        "symptoms": ["environment: coastal", "earth_leak_ma gradually increasing", "contactor_cycles normal but error_count rising", "connector resistance increasing (power_w declining)"],
        "resolution": "Deep clean all connectors. Apply anti-corrosion treatment. Seal PCBs with conformal coating. Schedule 6-monthly maintenance for coastal units (vs 12-monthly inland).",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "ENV-002", "pattern_name": "Condensation damage (temperature cycling)",
        "category": "environmental", "severity": "degraded",
        "root_cause": "Rapid temperature changes (cold nights, warm days) cause condensation inside the charger enclosure. Moisture on PCBs causes intermittent faults.",
        "symptoms": ["errors concentrated in early morning hours", "earth_leak_ma spikes at dawn", "temp_c shows large daily swing > 25C", "clears by midday as unit warms"],
        "resolution": "Install internal heater element or desiccant packs. Improve ventilation. For persistent cases, install enclosure with active climate control.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "ENV-003", "pattern_name": "Lightning-induced surge damage",
        "category": "environmental", "severity": "offline",
        "root_cause": "Nearby lightning strike causes voltage surge through mains supply. Damages power electronics, communication boards, or both.",
        "symptoms": ["sudden transition to Faulted or Unavailable during thunderstorm", "multiple components fail simultaneously", "error_code: InternalError", "often affects multiple chargers at same site"],
        "resolution": "Replace damaged modules. Install Type 2 surge protection device (SPD) at distribution board. Verify earth bonding. Check if existing SPD sacrificial elements need replacement.",
        "affected_models": None,
        "affected_firmware": None,
    },
    {
        "pattern_id": "ENV-004", "pattern_name": "Insect/rodent nest in enclosure",
        "category": "environmental", "severity": "degraded",
        "root_cause": "Small animals or insects build nests inside charger enclosure, blocking airflow and potentially causing short circuits.",
        "symptoms": ["temp_c rising with no load change", "fan_rpm normal but cooling ineffective", "intermittent InternalError", "seasonal pattern (spring/autumn)"],
        "resolution": "Remove nest and clean enclosure. Install mesh guards on ventilation openings. Schedule seasonal inspections for rural/outdoor sites.",
        "affected_models": None,
        "affected_firmware": None,
    },
]


def generate_sql():
    print("-- Seed data for outage_catalog")
    print("-- 24 curated failure patterns for similarity search")
    print()
    print("INSERT INTO outage_catalog")
    print("  (pattern_id, pattern_name, category, root_cause, symptoms,")
    print("   resolution, severity, affected_models, affected_firmware,")
    print("   occurrence_count, last_seen)")
    print("VALUES")

    rows = []
    for p in PATTERNS:
        symptoms_json = json.dumps(p["symptoms"]).replace("'", "\\'")
        models_json = json.dumps(p["affected_models"]).replace("'", "\\'") if p["affected_models"] else "NULL"
        fw_json = json.dumps(p["affected_firmware"]).replace("'", "\\'") if p["affected_firmware"] else "NULL"

        root_cause = p["root_cause"].replace("'", "\\'")
        resolution = p["resolution"].replace("'", "\\'")

        models_val = f"'{models_json}'" if p["affected_models"] else "NULL"
        fw_val = f"'{fw_json}'" if p["affected_firmware"] else "NULL"

        rows.append(
            f"  ('{p['pattern_id']}', '{p['pattern_name']}', '{p['category']}', "
            f"'{root_cause}', '{symptoms_json}', "
            f"'{resolution}', '{p['severity']}', "
            f"{models_val}, {fw_val}, "
            f"{__import__('random').randint(5, 200)}, "
            f"'{(__import__('datetime').date.today() - __import__('datetime').timedelta(days=__import__('random').randint(1, 90))).isoformat()}')"
        )

    print(",\n".join(rows) + ";")


if __name__ == "__main__":
    generate_sql()
