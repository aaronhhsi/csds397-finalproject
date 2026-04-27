"""
Ingest CDC PLACES data from the manually downloaded full CSV.

Download the county-level CSV from https://www.cdc.gov/places/
and save it to data/raw/places_full.csv before running this step.

This replaces the Socrata API ingestion entirely and writes to places_raw.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import RAW_DIR, PLACES_MEASURES
from database.db_utils import init_db, upsert_dataframe

CSV_PATH = RAW_DIR / "places_full.csv"

COL_CANDIDATES = {
    "fips":        ["locationid", "LocationID", "fips", "FIPS", "CountyFIPS"],
    "state_abbr":  ["stateabbr", "StateAbbr", "state_abbr", "State"],
    "county_name": ["locationname", "LocationName", "county_name", "CountyName"],
    "measure_id":  ["measureid", "MeasureId", "measure_id", "MeasureID"],
    "data_value":  ["data_value", "Data_Value", "DataValue"],
    "low_ci":      ["low_confidence_limit", "Low_Confidence_Limit", "LowConfidenceLimit"],
    "high_ci":     ["high_confidence_limit", "High_Confidence_Limit", "HighConfidenceLimit"],
    "population":  ["totalpopulation", "TotalPopulation", "Total_Population"],
}


def _find(columns: list[str], candidates: list[str]) -> str | None:
    col_lower = {c.lower(): c for c in columns}
    for candidate in candidates:
        if candidate.lower() in col_lower:
            return col_lower[candidate.lower()]
    return None


def load_and_normalise(path: Path) -> pd.DataFrame:
    print(f"[PLACES CSV] Reading {path} …")
    raw = pd.read_csv(path, dtype=str, low_memory=False)
    print(f"[PLACES CSV] {len(raw):,} rows loaded")

    cols = list(raw.columns)

    # Filter to county-level rows
    geo_col = _find(cols, ["geographiclevel", "GeographicLevel", "geographic_level"])
    if geo_col:
        raw = raw[raw[geo_col].str.strip().str.lower() == "county"]
        print(f"[PLACES CSV] {len(raw):,} rows after county filter")

    # Filter to target measures
    measure_col = _find(cols, COL_CANDIDATES["measure_id"])
    if measure_col:
        raw = raw[raw[measure_col].isin(PLACES_MEASURES)]
        print(f"[PLACES CSV] {len(raw):,} rows after measure filter {PLACES_MEASURES}")

    # Rename to schema column names
    rename = {}
    for target, candidates in COL_CANDIDATES.items():
        src = _find(list(raw.columns), candidates)
        if src:
            rename[src] = target
        else:
            print(f"[PLACES CSV] WARNING: no column found for '{target}' — will be null")

    df = raw.rename(columns=rename)
    keep = [c for c in COL_CANDIDATES.keys() if c in df.columns]
    df = df[keep].copy()

    df["fips"]       = df["fips"].astype(str).str.strip().str.zfill(5)
    df["data_value"] = pd.to_numeric(df["data_value"], errors="coerce")
    for col in ["low_ci", "high_ci", "population"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df[df["data_value"].between(0, 100)]
    df = df.dropna(subset=["fips", "measure_id"])
    df = df.drop_duplicates(subset=["fips", "measure_id"], keep="first")

    return df


def run() -> pd.DataFrame:
    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"{CSV_PATH} not found.\n"
            "Download the full PLACES county CSV from https://www.cdc.gov/places/ "
            f"and save it to {CSV_PATH}"
        )

    init_db()
    df = load_and_normalise(CSV_PATH)
    print(f"[PLACES CSV] {len(df):,} rows, "
          f"{df['state_abbr'].nunique() if 'state_abbr' in df.columns else '?'} states")

    upsert_dataframe(df, "places_raw", if_exists="replace")
    return df


if __name__ == "__main__":
    run()
