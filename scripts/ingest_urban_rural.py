"""
Ingest NCHS Urban-Rural Classification Scheme (2013) from CDC.

Classification codes:
  1 = Large central metro (population ≥ 1 M, county in centre)
  2 = Large fringe metro  (population ≥ 1 M, outer county)
  3 = Medium metro        (250 000 – 999 999)
  4 = Small metro         (50 000 – 249 999)
  5 = Micropolitan        (non-metro, 10 000 – 49 999)
  6 = Non-core (rural)    (non-metro, < 10 000)

Codes 1–4 are classified as "urban" in this project (see config.URBAN_CODES).

Reference:
  https://www.cdc.gov/nchs/data-analysis-tools/urban-rural.html
"""

import sys
from pathlib import Path
from io import BytesIO

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import NCHS_URBAN_URL, URBAN_CODES, RAW_DIR
from database.db_utils import init_db, upsert_dataframe


def _to_fips(val) -> str | None:
    """Convert any numeric or string FIPS representation to a zero-padded 5-char string."""
    if pd.isna(val):
        return None
    try:
        # handles floats like 1001.0 → "01001"
        return str(int(float(val))).zfill(5)
    except (ValueError, TypeError):
        s = str(val).strip()
        return s.zfill(5) if s.isdigit() else None


def download_urban_rural(url: str = NCHS_URBAN_URL) -> pd.DataFrame:
    print(f"[UR] Downloading {url} …")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()

    raw_path = RAW_DIR / "nchs_urban_rural.xlsx"
    raw_path.write_bytes(resp.content)
    print(f"[UR] Saved raw file → {raw_path}")

    df = pd.read_excel(BytesIO(resp.content), sheet_name=0, header=0)
    df.columns = [str(c).strip() for c in df.columns]
    print(f"[UR] Raw columns: {list(df.columns)}")
    print(f"[UR] First row:   {df.iloc[0].to_dict()}")
    return df


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    col_list = list(df.columns)
    col_lower = [c.lower() for c in col_list]

    # ── Identify FIPS column ──────────────────────────────────────────────────
    fips_col = next(
        (col_list[i] for i, c in enumerate(col_lower) if "fips" in c),
        col_list[0]   # positional fallback: first column
    )

    # ── Identify 2013 classification code column ──────────────────────────────
    # Try name-based detection first
    code_col = next(
        (col_list[i] for i, c in enumerate(col_lower) if "2013" in c and "class" in c),
        None
    )
    if code_col is None:
        code_col = next(
            (col_list[i] for i, c in enumerate(col_lower) if "2013" in c),
            None
        )
    if code_col is None:
        # Positional fallback: find a column whose values are mostly 1–6 integers
        for col in col_list:
            numeric = pd.to_numeric(df[col], errors="coerce")
            valid = numeric.dropna()
            if len(valid) > 0 and valid.between(1, 6).mean() > 0.8:
                code_col = col
                break

    # ── Identify optional state / county columns ──────────────────────────────
    state_col = next(
        (col_list[i] for i, c in enumerate(col_lower)
         if ("state" in c or "abr" in c or "abbr" in c) and "name" not in c),
        None
    )
    county_col = next(
        (col_list[i] for i, c in enumerate(col_lower) if "county" in c and "name" in c),
        next((col_list[i] for i, c in enumerate(col_lower) if "county" in c), None)
    )

    print(f"[UR] Mapping  fips={fips_col!r}, code={code_col!r}, "
          f"state={state_col!r}, county={county_col!r}")

    if code_col is None:
        raise ValueError(
            "Could not identify the urban-rural classification code column.\n"
            f"Available columns: {col_list}"
        )

    out = pd.DataFrame()
    out["fips"]        = df[fips_col].apply(_to_fips)
    out["ur_code"]     = pd.to_numeric(df[code_col], errors="coerce").astype("Int64")
    out["state_abbr"]  = df[state_col].astype(str).str.strip() if state_col else None
    out["county_name"] = df[county_col].astype(str).str.strip() if county_col else None
    out["is_urban"]    = out["ur_code"].isin(URBAN_CODES).astype(int)

    before = len(out)
    out = out.dropna(subset=["fips", "ur_code"])
    out = out[out["fips"].str.match(r"^\d{5}$")]
    out = out[out["ur_code"].between(1, 6)]
    print(f"[UR] {before} → {len(out)} rows after cleaning "
          f"(dropped {before - len(out)} invalid)")
    return out.drop_duplicates(subset=["fips"])


def run() -> pd.DataFrame:
    init_db()
    raw = download_urban_rural()
    normalised = _normalise(raw)
    upsert_dataframe(normalised, "urban_rural", if_exists="replace")
    print(f"[UR] Urban counties: {normalised['is_urban'].sum():,} / {len(normalised):,}")
    return normalised


if __name__ == "__main__":
    run()
