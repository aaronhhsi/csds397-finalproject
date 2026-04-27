"""
Supplement CDC PLACES API data with a manually downloaded CSV.

Use this when certain states (e.g. KY, PA) are missing from the Socrata API
but present in the full CSV export from https://www.cdc.gov/places/

Steps:
  1. Download the full PLACES county CSV from the CDC website
  2. Save it to data/raw/places_full.csv
  3. Run this script:  python scripts/ingest_places_csv.py
  4. Re-run the pipeline from step 5:  python run_pipeline.py --step 5

Only rows whose FIPS codes are NOT already in places_raw are inserted,
so running this multiple times is safe.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import RAW_DIR, PLACES_MEASURES
from database.db_utils import init_db, read_table, upsert_dataframe

CSV_PATH = RAW_DIR / "places_full.csv"

# Column name candidates for each target field (CDC changes names between releases)
COL_CANDIDATES = {
    "fips":        ["locationid", "LocationID", "fips", "FIPS", "CountyFIPS"],
    "state_abbr":  ["stateabbr", "StateAbbr", "state_abbr", "State"],
    "county_name": ["locationname", "LocationName", "county_name", "CountyName"],
    "measure_id":  ["measureid", "MeasureId", "measure_id", "MeasureID"],
    "data_value":  ["data_value", "Data_Value", "DataValue"],
    "low_ci":      ["low_confidence_limit", "Low_Confidence_Limit", "LowConfidenceLimit"],
    "high_ci":     ["high_confidence_limit", "High_Confidence_Limit", "HighConfidenceLimit"],
    "population":  ["totalpopulation", "TotalPopulation", "Total_Population"],
    "year":        ["year", "Year", "data_year", "DataYear"],
}


def _find(columns: list[str], candidates: list[str]) -> str | None:
    col_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in col_lower:
            return col_lower[candidate.lower()]
    return None


def load_and_normalise(path: Path) -> pd.DataFrame:
    print(f"[CSV] Reading {path} …")
    # Read with low_memory=False to avoid dtype warnings on large files
    raw = pd.read_csv(path, dtype=str, low_memory=False)
    print(f"[CSV] {len(raw):,} rows, columns: {list(raw.columns[:8])} …")

    cols = list(raw.columns)

    # ── Filter to county-level rows ───────────────────────────────────────────
    geo_col = _find(cols, ["geographiclevel", "GeographicLevel", "geographic_level"])
    if geo_col:
        raw = raw[raw[geo_col].str.strip().str.lower() == "county"]
        print(f"[CSV] {len(raw):,} rows after county filter")

    # ── Filter to target measures ─────────────────────────────────────────────
    measure_col = _find(cols, COL_CANDIDATES["measure_id"])
    if measure_col:
        raw = raw[raw[measure_col].isin(PLACES_MEASURES)]
        print(f"[CSV] {len(raw):,} rows after measure filter ({PLACES_MEASURES})")

    # ── Rename columns to schema names ────────────────────────────────────────
    rename = {}
    for target, candidates in COL_CANDIDATES.items():
        src = _find(list(raw.columns), candidates)
        if src:
            rename[src] = target
        else:
            print(f"[CSV] WARNING: no column found for '{target}' — will be null")

    df = raw.rename(columns=rename)

    # Keep only schema columns that exist
    keep = [c for c in COL_CANDIDATES.keys() if c in df.columns]
    df = df[keep].copy()

    # ── Type conversions ──────────────────────────────────────────────────────
    df["fips"] = df["fips"].astype(str).str.strip().str.zfill(5)
    for num_col in ["data_value", "low_ci", "high_ci", "population", "year"]:
        if num_col in df.columns:
            df[num_col] = pd.to_numeric(df[num_col], errors="coerce")

    df = df[df["data_value"].between(0, 100)]
    df = df.dropna(subset=["fips", "measure_id"])
    df = df.drop_duplicates(subset=["fips", "measure_id"])

    return df


def run() -> None:
    if not CSV_PATH.exists():
        print(f"[CSV] ERROR: {CSV_PATH} not found.")
        print("  Download the full PLACES county CSV from https://www.cdc.gov/places/")
        print(f"  and save it to {CSV_PATH}")
        return

    init_db()

    new_data = load_and_normalise(CSV_PATH)
    print(f"[CSV] {len(new_data):,} normalised rows from CSV")

    # ── Find FIPS already in places_raw ───────────────────────────────────────
    existing = read_table("places_raw")
    existing_fips = set(existing["fips"].astype(str).str.zfill(5).unique())
    print(f"[CSV] {len(existing_fips):,} FIPS codes already in places_raw")

    # ── Keep only rows with new FIPS codes ────────────────────────────────────
    to_add = new_data[~new_data["fips"].isin(existing_fips)]
    print(f"[CSV] {len(to_add):,} new rows to insert "
          f"({to_add['state_abbr'].nunique() if 'state_abbr' in to_add.columns else '?'} states)")

    if to_add.empty:
        print("[CSV] Nothing to add — all FIPS already present in places_raw")
        return

    if "state_abbr" in to_add.columns:
        print(f"[CSV] New states: {sorted(to_add['state_abbr'].dropna().unique())}")

    upsert_dataframe(to_add, "places_raw", if_exists="append")
    print("[CSV] Done. Now run:  python run_pipeline.py --step 5")


if __name__ == "__main__":
    run()
