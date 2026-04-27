"""
Merge cleaned datasets and build the final county_analysis table.

Join strategy (all on 5-digit FIPS):
  PLACES (inactivity, obesity, smoking)
  + CHR   (park access, life expectancy)
  + Urban-Rural (ur_code, is_urban)

Derived / engineered features:
  - inactivity_zscore: z-score of inactivity_rate within urban counties
  - le_deficit: national median life expectancy minus county life expectancy
  - park_quartile: quartile bin of park_access_pct (Q1–Q4)

PLACES is the left spine. CHR and Urban-Rural are left-joined so counties
missing from either source still appear (useful for the national map).
Counties used in H1/H2 analysis are those with non-null CHR values.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_utils import read_table, upsert_dataframe, get_engine
from config import PROCESSED_DIR
from sqlalchemy import text


def _load_clean_places() -> pd.DataFrame:
    """Pivot PLACES from the database (or reuse the CSV if available)."""
    csv = PROCESSED_DIR / "places_clean.csv"
    if csv.exists():
        return pd.read_csv(csv, dtype={"fips": str})

    # Rebuild pivot on the fly
    df = read_table("places_raw")
    df = df.dropna(subset=["fips", "measure_id", "data_value"])
    df = df[df["data_value"].between(0, 100)]
    df = df.drop_duplicates(subset=["fips", "measure_id"])
    wide = (df.pivot_table(
                index=["fips", "state_abbr", "county_name"],
                columns="measure_id",
                values="data_value",
                aggfunc="first")
            .reset_index())
    wide.columns.name = None
    return wide.rename(columns={
        "LPA":      "inactivity_rate",
        "OBESITY":  "obesity_rate",
        "CSMOKING": "smoking_rate",
    })


def _load_clean_chr() -> pd.DataFrame:
    csv = PROCESSED_DIR / "chr_clean.csv"
    if csv.exists():
        return pd.read_csv(csv, dtype={"fips": str})
    return read_table("chr_raw")


def _load_clean_ur() -> pd.DataFrame:
    csv = PROCESSED_DIR / "urban_rural_clean.csv"
    if csv.exists():
        return pd.read_csv(csv, dtype={"fips": str})
    return read_table("urban_rural")


def merge_datasets() -> pd.DataFrame:
    places = _load_clean_places()
    chr_df  = _load_clean_chr()
    ur      = _load_clean_ur()

    # PLACES × CHR: left join so counties missing from CHR still appear on the map
    df = places.merge(
        chr_df[["fips", "park_access_pct", "life_expectancy"]],
        on="fips", how="left"
    )
    matched = df["park_access_pct"].notna().sum()
    print(f"[merge] {len(df):,} total counties, {matched:,} with CHR data")

    # Urban-rural: left join so missing codes don't discard counties
    df = df.merge(
        ur[["fips", "ur_code", "is_urban"]],
        on="fips", how="left"
    )
    df["ur_code"]  = df["ur_code"].fillna(-1).astype(int)
    df["is_urban"] = df["is_urban"].fillna(0).astype(int)
    missing_ur = (df["ur_code"] == -1).sum()
    if missing_ur:
        print(f"[merge] WARNING: {missing_ur:,} counties have no urban-rural code")
    print(f"[merge] {len(df):,} counties after left-join urban-rural")
    return df


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    # z-score of inactivity among urban counties only
    urban_mask = df["is_urban"] == 1
    urban_mean = df.loc[urban_mask, "inactivity_rate"].mean()
    urban_std  = df.loc[urban_mask, "inactivity_rate"].std()
    df["inactivity_zscore"] = (df["inactivity_rate"] - urban_mean) / urban_std

    # life-expectancy deficit from national median
    nat_median = df["life_expectancy"].median()
    df["le_deficit"] = nat_median - df["life_expectancy"]

    # park access quartile label
    try:
        df["park_quartile"] = pd.qcut(
            df["park_access_pct"], q=4,
            labels=["Q1 (lowest)", "Q2", "Q3", "Q4 (highest)"],
            duplicates="drop"
        ).astype(str)
    except ValueError:
        # Fewer than 4 unique values — fall back to equal-width bins
        df["park_quartile"] = pd.cut(
            df["park_access_pct"], bins=4,
            labels=["Q1 (lowest)", "Q2", "Q3", "Q4 (highest)"]
        ).astype(str)

    # Urban-rural label
    label_map = {
        1: "Large central metro",
        2: "Large fringe metro",
        3: "Medium metro",
        4: "Small metro",
        5: "Micropolitan",
        6: "Non-core (rural)",
    }
    df["ur_label"] = df["ur_code"].map(label_map)

    return df


def run() -> pd.DataFrame:
    df = merge_datasets()
    df = engineer_features(df)

    # Select final columns for county_analysis table
    final_cols = [
        "fips", "state_abbr", "county_name",
        "ur_code", "ur_label", "is_urban",
        "park_access_pct", "inactivity_rate", "obesity_rate", "smoking_rate",
        "life_expectancy",
        "inactivity_zscore", "le_deficit", "park_quartile",
    ]
    # Keep only columns that actually exist
    final_cols = [c for c in final_cols if c in df.columns]
    out = df[final_cols].copy()

    # Write to DB (replace full table each run)
    upsert_dataframe(out, "county_analysis", if_exists="replace")

    # Also save as CSV for dashboard fallback
    csv_path = PROCESSED_DIR / "county_analysis.csv"
    out.to_csv(csv_path, index=False)
    print(f"[merge] county_analysis: {len(out):,} rows → DB + {csv_path}")
    return out


if __name__ == "__main__":
    result = run()
    print("\nSample output:")
    print(result.head(5).to_string())
    print("\nMissing value summary:")
    print(result.isna().sum())
