"""
Clean the CDC PLACES data already loaded into places_raw.

Steps:
  1. Pivot from long format (one row per measure) to wide format (one row per county).
  2. Drop counties with data_value outside plausible range (0–100 %).
  3. Flag and remove duplicate FIPS / measure pairs (keep most recent year).
  4. Write cleaned wide-format data back to database (used by merge step).
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_utils import read_table, upsert_dataframe, get_engine
from config import PROCESSED_DIR


MEASURE_RENAME = {
    "LPA":      "inactivity_rate",
    "OBESITY":  "obesity_rate",
    "CSMOKING": "smoking_rate",
}


def clean_places() -> pd.DataFrame:
    df = read_table("places_raw")
    print(f"[clean_PLACES] Input rows: {len(df):,}")

    # ── 1. Drop rows with missing or out-of-range values ─────────────────────
    df = df.dropna(subset=["fips", "measure_id", "data_value"])
    df = df[df["data_value"].between(0, 100)]

    # ── 2. Keep the most recent year when a county appears twice ─────────────
    df = (df.sort_values("year", ascending=False)
            .drop_duplicates(subset=["fips", "measure_id"], keep="first"))

    # ── 3. Pivot to wide format ───────────────────────────────────────────────
    wide = (df.pivot_table(
                index=["fips", "state_abbr", "county_name"],
                columns="measure_id",
                values="data_value",
                aggfunc="first",
            )
            .reset_index())
    wide.columns.name = None
    wide = wide.rename(columns=MEASURE_RENAME)

    # ── 4. Sanity checks ──────────────────────────────────────────────────────
    before = len(wide)
    wide = wide.dropna(subset=list(MEASURE_RENAME.values()), how="all")
    print(f"[clean_PLACES] Rows after pivot & dedup: {len(wide):,} "
          f"(dropped {before - len(wide):,} with all-null measures)")

    # ── 5. Save to DB and CSV ─────────────────────────────────────────────────
    upsert_dataframe(wide, "places_clean", if_exists="replace")
    out_path = PROCESSED_DIR / "places_clean.csv"
    wide.to_csv(out_path, index=False)
    print(f"[clean_PLACES] Saved → DB (places_clean) + {out_path}")
    return wide


if __name__ == "__main__":
    result = clean_places()
    print(result.head())
    print(result.describe())
