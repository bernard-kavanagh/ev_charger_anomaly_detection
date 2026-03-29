"""
Seed generator: charger_registry
=================================
Generates 20,000 realistic EV charge point records spread across Irish sites.
Outputs SQL INSERT statements or CSV for bulk loading.

Usage:
    python seed_charger_registry.py --format sql > seed_registry.sql
    python seed_charger_registry.py --format csv > seed_registry.csv
"""

import argparse
import csv
import io
import json
import random
from datetime import date, timedelta

# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

MANUFACTURERS = [
    {"name": "ABB", "models": [
        {"model": "Terra 54", "max_kw": 50.0, "connectors": 1},
        {"model": "Terra 124", "max_kw": 120.0, "connectors": 2},
        {"model": "Terra 184", "max_kw": 180.0, "connectors": 2},
        {"model": "Terra 360", "max_kw": 360.0, "connectors": 2},
    ]},
    {"name": "Tritium", "models": [
        {"model": "RT50", "max_kw": 50.0, "connectors": 1},
        {"model": "PKM150", "max_kw": 150.0, "connectors": 1},
        {"model": "RTM75", "max_kw": 75.0, "connectors": 2},
    ]},
    {"name": "Alpitronic", "models": [
        {"model": "Hypercharger 150", "max_kw": 150.0, "connectors": 2},
        {"model": "Hypercharger 300", "max_kw": 300.0, "connectors": 4},
        {"model": "Hypercharger 400", "max_kw": 400.0, "connectors": 4},
    ]},
    {"name": "EVBox", "models": [
        {"model": "Elvi 11", "max_kw": 11.0, "connectors": 1},
        {"model": "Elvi 22", "max_kw": 22.0, "connectors": 1},
        {"model": "Troniq 50", "max_kw": 50.0, "connectors": 2},
        {"model": "Troniq 100", "max_kw": 100.0, "connectors": 2},
    ]},
    {"name": "Wallbox", "models": [
        {"model": "Pulsar Plus 7", "max_kw": 7.4, "connectors": 1},
        {"model": "Pulsar Plus 22", "max_kw": 22.0, "connectors": 1},
        {"model": "Supernova 65", "max_kw": 65.0, "connectors": 2},
    ]},
    {"name": "Kempower", "models": [
        {"model": "S-Series 40", "max_kw": 40.0, "connectors": 1},
        {"model": "T-Series 200", "max_kw": 200.0, "connectors": 3},
        {"model": "S-Series 240", "max_kw": 240.0, "connectors": 4},
    ]},
]

# Firmware versions per manufacturer (some deliberately older for testing)
FIRMWARE_VERSIONS = {
    "ABB":        ["3.1.2", "3.1.5", "3.2.0", "3.2.1", "3.3.0"],
    "Tritium":    ["2.4.1", "2.5.0", "2.5.3", "2.6.0"],
    "Alpitronic": ["1.8.0", "1.9.2", "1.10.0", "1.10.1"],
    "EVBox":      ["4.0.3", "4.1.0", "4.1.2", "4.2.0"],
    "Wallbox":    ["5.2.1", "5.3.0", "5.3.2", "5.4.0"],
    "Kempower":   ["1.2.0", "1.3.1", "1.4.0", "1.4.2"],
}

# Irish sites: county-based with lat/lon centres and environment distribution
SITES = [
    # Dublin (high density: ~4000 chargers)
    {"prefix": "DUB", "county": "Dublin", "lat": 53.3498, "lon": -6.2603, "count": 1200, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.5, "outdoor_exposed": 0.25, "coastal": 0.05}},
    {"prefix": "DUB-NORTH", "county": "Dublin", "lat": 53.3900, "lon": -6.2100, "count": 900, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.45, "outdoor_exposed": 0.3, "coastal": 0.1}},
    {"prefix": "DUB-SOUTH", "county": "Dublin", "lat": 53.2900, "lon": -6.1800, "count": 900, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.5, "outdoor_exposed": 0.2, "coastal": 0.1}},
    {"prefix": "DUB-WEST", "county": "Dublin", "lat": 53.3500, "lon": -6.4200, "count": 800, "envs": {"indoor": 0.25, "outdoor_sheltered": 0.5, "outdoor_exposed": 0.25, "coastal": 0.0}},
    {"prefix": "DUB-DOCKS", "county": "Dublin", "lat": 53.3450, "lon": -6.2300, "count": 800, "envs": {"indoor": 0.3, "outdoor_sheltered": 0.4, "outdoor_exposed": 0.2, "coastal": 0.1}},
    {"prefix": "DUB-AIRPORT", "county": "Dublin", "lat": 53.4264, "lon": -6.2499, "count": 600, "envs": {"indoor": 0.4, "outdoor_sheltered": 0.5, "outdoor_exposed": 0.1, "coastal": 0.0}},
    {"prefix": "DUN-LAOGH", "county": "Dublin", "lat": 53.2940, "lon": -6.1340, "count": 500, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.3, "coastal": 0.3}},
    {"prefix": "SWORDS", "county": "Dublin", "lat": 53.4597, "lon": -6.2181, "count": 500, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.5, "outdoor_exposed": 0.3, "coastal": 0.0}},
    # Cork (~3000 chargers)
    {"prefix": "CORK-CITY", "county": "Cork", "lat": 51.8985, "lon": -8.4756, "count": 1200, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.4, "outdoor_exposed": 0.3, "coastal": 0.1}},
    {"prefix": "CORK-EAST", "county": "Cork", "lat": 51.8500, "lon": -8.2000, "count": 600, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.4, "coastal": 0.2}},
    {"prefix": "CORK-WEST", "county": "Cork", "lat": 51.7500, "lon": -8.8000, "count": 500, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.25, "outdoor_exposed": 0.4, "coastal": 0.3}},
    {"prefix": "CORK-RING", "county": "Cork", "lat": 51.6900, "lon": -8.5300, "count": 500, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.35, "coastal": 0.25}},
    # Galway (~2000 chargers)
    {"prefix": "GALWAY-CITY", "county": "Galway", "lat": 53.2707, "lon": -9.0568, "count": 900, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.3, "coastal": 0.2}},
    {"prefix": "GALWAY-COAST", "county": "Galway", "lat": 53.2300, "lon": -9.3000, "count": 500, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.2, "outdoor_exposed": 0.35, "coastal": 0.4}},
    {"prefix": "GALWAY-EAST", "county": "Galway", "lat": 53.3500, "lon": -8.7500, "count": 500, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.5, "coastal": 0.1}},
    # Limerick (~1500 chargers)
    {"prefix": "LMK-CITY", "county": "Limerick", "lat": 52.6638, "lon": -8.6267, "count": 900, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.4, "outdoor_exposed": 0.35, "coastal": 0.05}},
    {"prefix": "LMK-RING", "county": "Limerick", "lat": 52.6000, "lon": -8.7500, "count": 500, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.5, "coastal": 0.1}},
    # Waterford (~1000 chargers)
    {"prefix": "WFORD-CITY", "county": "Waterford", "lat": 52.2593, "lon": -7.1101, "count": 600, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.3, "coastal": 0.2}},
    {"prefix": "WFORD-COAST", "county": "Waterford", "lat": 52.1500, "lon": -7.3500, "count": 300, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.2, "outdoor_exposed": 0.35, "coastal": 0.4}},
    # Kilkenny, Wexford, Kildare, Wicklow, Meath etc (~4000+ remaining)
    {"prefix": "KILKENNY", "county": "Kilkenny", "lat": 52.6541, "lon": -7.2448, "count": 500, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.45, "coastal": 0.05}},
    {"prefix": "WEXFORD", "county": "Wexford", "lat": 52.3369, "lon": -6.4633, "count": 500, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.35, "coastal": 0.25}},
    {"prefix": "KILDARE", "county": "Kildare", "lat": 53.1589, "lon": -6.9096, "count": 800, "envs": {"indoor": 0.2, "outdoor_sheltered": 0.45, "outdoor_exposed": 0.35, "coastal": 0.0}},
    {"prefix": "WICKLOW", "county": "Wicklow", "lat": 52.9808, "lon": -6.0448, "count": 600, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.4, "coastal": 0.2}},
    {"prefix": "MEATH", "county": "Meath", "lat": 53.6054, "lon": -6.6564, "count": 600, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.4, "outdoor_exposed": 0.4, "coastal": 0.05}},
    {"prefix": "DROGHEDA", "county": "Louth", "lat": 53.7179, "lon": -6.3561, "count": 500, "envs": {"indoor": 0.15, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.35, "coastal": 0.15}},
    {"prefix": "ATHLONE", "county": "Westmeath", "lat": 53.4233, "lon": -7.9407, "count": 300, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.55, "coastal": 0.0}},
    {"prefix": "SLIGO", "county": "Sligo", "lat": 54.2766, "lon": -8.4761, "count": 300, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.25, "outdoor_exposed": 0.35, "coastal": 0.3}},
    {"prefix": "DONEGAL", "county": "Donegal", "lat": 54.6538, "lon": -8.1096, "count": 300, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.2, "outdoor_exposed": 0.35, "coastal": 0.4}},
    {"prefix": "KERRY", "county": "Kerry", "lat": 52.0599, "lon": -9.5044, "count": 500, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.2, "outdoor_exposed": 0.35, "coastal": 0.4}},
    {"prefix": "CLARE", "county": "Clare", "lat": 52.8432, "lon": -8.9815, "count": 300, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.25, "outdoor_exposed": 0.35, "coastal": 0.35}},
    {"prefix": "SHANNON", "county": "Clare", "lat": 52.7019, "lon": -8.8644, "count": 300, "envs": {"indoor": 0.3, "outdoor_sheltered": 0.4, "outdoor_exposed": 0.25, "coastal": 0.05}},
    {"prefix": "MAYO", "county": "Mayo", "lat": 53.7633, "lon": -9.2988, "count": 300, "envs": {"indoor": 0.05, "outdoor_sheltered": 0.2, "outdoor_exposed": 0.35, "coastal": 0.4}},
    {"prefix": "TIPPERARY", "county": "Tipperary", "lat": 52.4735, "lon": -8.1622, "count": 300, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.35, "outdoor_exposed": 0.5, "coastal": 0.05}},
    {"prefix": "ROSCOMMON", "county": "Roscommon", "lat": 53.6272, "lon": -8.1897, "count": 200, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.6, "coastal": 0.0}},
    {"prefix": "LEITRIM", "county": "Leitrim", "lat": 54.1235, "lon": -8.0030, "count": 200, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.6, "coastal": 0.0}},
    {"prefix": "CAVAN", "county": "Cavan", "lat": 53.9907, "lon": -7.3600, "count": 100, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.6, "coastal": 0.0}},
    {"prefix": "MONAGHAN", "county": "Monaghan", "lat": 54.2492, "lon": -6.9683, "count": 100, "envs": {"indoor": 0.1, "outdoor_sheltered": 0.3, "outdoor_exposed": 0.6, "coastal": 0.0}},
]


def pick_environment(env_dist: dict) -> str:
    r = random.random()
    cumulative = 0.0
    for env, prob in env_dist.items():
        cumulative += prob
        if r <= cumulative:
            return env
    return list(env_dist.keys())[-1]


def jitter_coords(lat: float, lon: float, radius_km: float = 5.0):
    """Add random jitter within radius_km."""
    lat_jitter = random.uniform(-radius_km / 111.0, radius_km / 111.0)
    lon_jitter = random.uniform(-radius_km / 78.0, radius_km / 78.0)
    return round(lat + lat_jitter, 6), round(lon + lon_jitter, 6)


def generate_chargers():
    """Generate all charger records."""
    chargers = []
    global_idx = 0

    for site in SITES:
        site_id = f"SITE-IE-{site['prefix']}"

        for i in range(site["count"]):
            global_idx += 1
            charger_id = f"CP-IE-{site['prefix']}-{global_idx:05d}"

            mfr = random.choice(MANUFACTURERS)
            model_info = random.choice(mfr["models"])
            fw = random.choice(FIRMWARE_VERSIONS[mfr["name"]])
            env = pick_environment(site["envs"])
            lat, lon = jitter_coords(site["lat"], site["lon"])

            # Install date: 1-5 years ago
            install_days_ago = random.randint(180, 1825)
            install_date = date.today() - timedelta(days=install_days_ago)

            # Last maintenance: 30-365 days ago
            maint_days_ago = random.randint(30, 365)
            last_maint = date.today() - timedelta(days=maint_days_ago)

            # Usage stats proportional to age and location density
            age_factor = install_days_ago / 365.0
            density_factor = site["count"] / 800.0  # normalise to Dublin
            total_sessions = int(random.gauss(
                1500 * age_factor * density_factor,
                400 * age_factor
            ))
            total_sessions = max(100, total_sessions)
            avg_kwh_per_session = random.uniform(15, 45) if model_info["max_kw"] > 22 else random.uniform(5, 18)
            total_energy_kwh = int(total_sessions * avg_kwh_per_session)

            chargers.append({
                "charger_id": charger_id,
                "site_id": site_id,
                "model": model_info["model"],
                "manufacturer": mfr["name"],
                "firmware_version": fw,
                "install_date": install_date.isoformat(),
                "lat": lat,
                "lon": lon,
                "connector_count": model_info["connectors"],
                "max_power_kw": model_info["max_kw"],
                "environment": env,
                "last_maintenance": last_maint.isoformat(),
                "total_sessions": total_sessions,
                "total_energy_kwh": total_energy_kwh,
            })

    return chargers


def to_sql(chargers):
    """Output as SQL INSERT statements (batched for performance)."""
    print("-- Seed data for charger_registry")
    print("-- Generated: 20,000 EV charge points across Ireland")
    print()

    batch_size = 500
    for i in range(0, len(chargers), batch_size):
        batch = chargers[i:i + batch_size]
        print("INSERT INTO charger_registry")
        print("  (charger_id, site_id, model, manufacturer, firmware_version,")
        print("   install_date, lat, lon, connector_count, max_power_kw,")
        print("   environment, last_maintenance, total_sessions, total_energy_kwh)")
        print("VALUES")

        rows = []
        for c in batch:
            rows.append(
                f"  ('{c['charger_id']}', '{c['site_id']}', '{c['model']}', "
                f"'{c['manufacturer']}', '{c['firmware_version']}', "
                f"'{c['install_date']}', {c['lat']}, {c['lon']}, "
                f"{c['connector_count']}, {c['max_power_kw']}, "
                f"'{c['environment']}', '{c['last_maintenance']}', "
                f"{c['total_sessions']}, {c['total_energy_kwh']})"
            )
        print(",\n".join(rows) + ";\n")


def to_csv(chargers):
    """Output as CSV."""
    writer = csv.DictWriter(
        io.TextIOWrapper(open("/dev/stdout", "wb"), encoding="utf-8"),
        fieldnames=chargers[0].keys()
    )
    writer.writeheader()
    writer.writerows(chargers)


def main():
    parser = argparse.ArgumentParser(description="Generate charger_registry seed data")
    parser.add_argument("--format", choices=["sql", "csv", "json"], default="sql")
    parser.add_argument("--count", type=int, help="Override total count (use site proportions)")
    args = parser.parse_args()

    random.seed(42)  # reproducible
    chargers = generate_chargers()

    print(f"-- Total chargers generated: {len(chargers)}", file=__import__("sys").stderr)

    if args.format == "sql":
        to_sql(chargers)
    elif args.format == "csv":
        to_csv(chargers)
    elif args.format == "json":
        print(json.dumps(chargers, indent=2))


if __name__ == "__main__":
    main()
