"""
Ingest County Health Rankings analytic data (wide-format CSV).

Downloads one CSV per year that contains hundreds of columns.  We extract:
  - park / exercise access (v011_rawvalue)
  - life expectancy         (v147_rawvalue)
  - physical inactivity     (v070_rawvalue)  — cross-check with PLACES
  - adult smoking           (v044_rawvalue)  — cross-check with PLACES

Run with --list-cols to print every column name in the downloaded file so you
can verify or update the column constants in config.py.

CHR data documentation:
  https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation
"""

import sys
import argparse
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    CHR_URL, CHR_YEAR,
    CHR_COL_PARK_ACCESS, CHR_COL_INACTIVITY,
    CHR_COL_SMOKING, CHR_COL_LIFE_EXP,
    RAW_DIR
)
from database.db_utils import init_db, upsert_dataframe


def download_chr(url: str = CHR_URL) -> pd.DataFrame:
    print(f"[CHR] Downloading {url} …")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    # CHR CSVs use the first row as a header description; row 1 = real headers.
    # Read the raw text and let pandas sort it out.
    raw_path = RAW_DIR / f"chr_{CHR_YEAR}_raw.csv"
    raw_path.write_bytes(resp.content)
    print(f"[CHR] Saved raw CSV → {raw_path}")

    # CHR puts a title row before the actual header; skip if first cell is not "fipscode"
    df = pd.read_csv(raw_path, dtype=str, low_memory=False)
    if df.columns[0].lower() != "fipscode":
        df = pd.read_csv(raw_path, skiprows=1, dtype=str, low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    print(f"[CHR] Downloaded {len(df):,} rows × {df.shape[1]} columns")
    return df


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Extract the columns we care about and standardise types."""
    fips_col = next((c for c in df.columns if "fips" in c), None)
    if fips_col is None:
        raise ValueError("Cannot find FIPS column in CHR data")

    keep = {
        fips_col:               "fips",
        "state":                "state_abbr",
        "county":               "county_name",
    }
    # Add measure columns — only include if present in downloaded file
    measure_map = {
        CHR_COL_PARK_ACCESS.lower(): "park_access_pct",
        CHR_COL_LIFE_EXP.lower():    "life_expectancy",
        CHR_COL_INACTIVITY.lower():  "inactivity_chk",
        CHR_COL_SMOKING.lower():     "smoking_chk",
    }
    for src, dst in measure_map.items():
        if src in df.columns:
            keep[src] = dst
        else:
            print(f"[CHR] WARNING: column '{src}' not found — skipping {dst}")

    existing = {k: v for k, v in keep.items() if k in df.columns}
    out = df[list(existing.keys())].rename(columns=existing).copy()

    out["fips"] = out["fips"].astype(str).str.zfill(5)

    # Drop state-level summary rows (FIPS ends in "000")
    out = out[~out["fips"].str.endswith("000")]

    for col in ["park_access_pct", "life_expectancy", "inactivity_chk", "smoking_chk"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            # CHR stores as 0–1 fractions; convert to 0–100 %
            if col != "life_expectancy":
                out[col] = out[col] * 100

    out["year"] = CHR_YEAR
    return out.drop_duplicates(subset=["fips"])


def run(list_cols: bool = False) -> pd.DataFrame:
    init_db()
    raw = download_chr()

    if list_cols:
        print("\nAll columns in CHR file:")
        for c in sorted(raw.columns):
            print(f"  {c}")
        return raw

    normalised = _normalise(raw)
    upsert_dataframe(normalised, "chr_raw", if_exists="replace")
    return normalised


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-cols", action="store_true",
                        help="Print all column names in the CHR CSV then exit")
    args = parser.parse_args()
    run(list_cols=args.list_cols)
