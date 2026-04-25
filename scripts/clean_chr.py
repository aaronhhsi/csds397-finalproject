"""
Clean the County Health Rankings data already loaded into chr_raw.

Steps:
  1. Remove counties with extreme or implausible values.
  2. Impute small gaps with the state median (< 10 % missing per column).
  3. Write cleaned data to processed/chr_clean.csv.
"""

import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_utils import read_table, upsert_dataframe
from config import PROCESSED_DIR

NUMERIC_COLS = ["park_access_pct", "life_expectancy", "inactivity_chk", "smoking_chk"]

VALID_RANGES = {
    "park_access_pct": (0, 100),
    "life_expectancy": (50, 100),   # plausible county LE in the US
    "inactivity_chk":  (0, 100),
    "smoking_chk":     (0, 100),
}


def clean_chr() -> pd.DataFrame:
    df = read_table("chr_raw")
    print(f"[clean_CHR] Input rows: {len(df):,}")

    # ── 1. Drop rows missing both park access AND life expectancy ─────────────
    df = df.dropna(subset=["park_access_pct", "life_expectancy"], how="all")

    # ── 2. Range filter ───────────────────────────────────────────────────────
    for col, (lo, hi) in VALID_RANGES.items():
        if col not in df.columns:
            continue
        bad = df[col].notna() & ~df[col].between(lo, hi)
        if bad.any():
            print(f"[clean_CHR] Nullifying {bad.sum()} out-of-range values in {col}")
            df.loc[bad, col] = np.nan

    # ── 3. State-median imputation (only when < 10 % missing per column) ──────
    for col in NUMERIC_COLS:
        if col not in df.columns:
            continue
        miss_rate = df[col].isna().mean()
        if miss_rate == 0:
            continue
        if miss_rate < 0.10:
            state_median = df.groupby("state_abbr")[col].transform("median")
            df[col] = df[col].fillna(state_median)
            print(f"[clean_CHR] Imputed {miss_rate:.1%} missing in {col} with state median")
        else:
            print(f"[clean_CHR] {col} has {miss_rate:.1%} missing — leaving as-is")

    # ── 4. Drop duplicate FIPS ────────────────────────────────────────────────
    df = df.drop_duplicates(subset=["fips"], keep="last")

    upsert_dataframe(df, "chr_clean", if_exists="replace")
    out_path = PROCESSED_DIR / "chr_clean.csv"
    df.to_csv(out_path, index=False)
    print(f"[clean_CHR] {len(df):,} counties saved → DB (chr_clean) + {out_path}")
    return df


if __name__ == "__main__":
    result = clean_chr()
    print(result[["fips", "county_name", "park_access_pct",
                  "life_expectancy"]].head(10).to_string())
