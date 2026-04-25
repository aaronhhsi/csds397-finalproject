"""
Validate and clean the NCHS Urban-Rural classification already in urban_rural.

Very little cleaning is needed (the source is a curated government table), but
we check for invalid codes, duplicate FIPS entries, and territories vs. states.
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.db_utils import read_table
from config import PROCESSED_DIR, URBAN_CODES

# Non-state FIPS prefixes to exclude (territories)
TERRITORY_FIPS_PREFIXES = {"60", "66", "69", "72", "78"}


def clean_urban_rural() -> pd.DataFrame:
    df = read_table("urban_rural")
    print(f"[clean_UR] Input rows: {len(df):,}")

    # ── 1. Keep only valid ur_codes (1–6) ─────────────────────────────────────
    df = df[df["ur_code"].between(1, 6)]

    # ── 2. Exclude US territories ─────────────────────────────────────────────
    before = len(df)
    df = df[~df["fips"].str[:2].isin(TERRITORY_FIPS_PREFIXES)]
    print(f"[clean_UR] Dropped {before - len(df)} territory rows")

    # ── 3. Dedup ───────────────────────────────────────────────────────────────
    df = df.drop_duplicates(subset=["fips"])

    # ── 4. Confirm is_urban flag matches URBAN_CODES ──────────────────────────
    df["is_urban"] = df["ur_code"].isin(URBAN_CODES).astype(int)

    out_path = PROCESSED_DIR / "urban_rural_clean.csv"
    df.to_csv(out_path, index=False)
    print(f"[clean_UR] {len(df):,} counties (urban={df['is_urban'].sum()}) → {out_path}")
    return df


if __name__ == "__main__":
    result = clean_urban_rural()
    print(result["ur_code"].value_counts().sort_index())
