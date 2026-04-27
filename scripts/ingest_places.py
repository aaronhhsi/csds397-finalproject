"""
Ingest CDC PLACES county-level health behavior data via the Socrata API.

Fetches physical inactivity (LPA), obesity (OBESITY), and smoking (CSMOKING)
estimates for all U.S. counties and writes them to places_raw in the database.

CDC PLACES dataset documentation:
  https://www.cdc.gov/places/index.html
Socrata API guide:
  https://dev.socrata.com/foundry/data.cdc.gov/{PLACES_DATASET_ID}
"""

import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import (
    PLACES_BASE_URL, PLACES_DATASET_ID, PLACES_APP_TOKEN,
    PLACES_MEASURES, PLACES_PAGE_SIZE, RAW_DIR
)
from database.db_utils import init_db, upsert_dataframe


def _base_headers() -> dict:
    h = {"Accept": "application/json"}
    if PLACES_APP_TOKEN:
        h["X-App-Token"] = PLACES_APP_TOKEN
    return h


def _sniff_columns(session: requests.Session, url: str) -> list[str]:
    """Fetch one row to discover actual column names the API exposes."""
    resp = session.get(url, params={"$limit": 1}, headers=_base_headers(), timeout=30)
    resp.raise_for_status()
    rows = resp.json()
    if not rows:
        return []
    return list(rows[0].keys())


def _find_col(candidates: list[str], *keywords: str) -> str | None:
    """Case-insensitive search for a column matching all keywords."""
    kw_lower = [k.lower() for k in keywords]
    for c in candidates:
        cl = c.lower()
        if all(k in cl for k in kw_lower):
            return c
    return None


def _fetch_all_pages(session: requests.Session, url: str,
                     where: str | None, order_col: str | None) -> list[dict]:
    """Paginate through the full dataset and return all rows."""
    rows: list[dict] = []
    offset = 0
    while True:
        params: dict = {"$limit": PLACES_PAGE_SIZE, "$offset": offset}
        if where:
            params["$where"] = where
        if order_col:
            params["$order"] = order_col
        resp = session.get(url, params=params, headers=_base_headers(), timeout=60)
        resp.raise_for_status()
        page = resp.json()
        if not page:
            break
        rows.extend(page)
        print(f"  … offset={offset}, got {len(page)} rows (total so far: {len(rows)})")
        if len(page) < PLACES_PAGE_SIZE:
            break
        offset += PLACES_PAGE_SIZE
    return rows


def fetch_places_data() -> pd.DataFrame:
    url = f"{PLACES_BASE_URL}/{PLACES_DATASET_ID}.json"

    with requests.Session() as session:
        print(f"[PLACES] Sniffing columns from {url} …")
        try:
            cols = _sniff_columns(session, url)
        except requests.HTTPError as e:
            raise RuntimeError(
                f"Cannot reach CDC PLACES dataset '{PLACES_DATASET_ID}'.\n"
                f"Visit https://data.cdc.gov and search 'PLACES County' to find "
                f"the current dataset ID, then update PLACES_DATASET_ID in config.py.\n"
                f"Original error: {e}"
            ) from e

        print(f"[PLACES] Columns found: {cols}")

        measure_col  = _find_col(cols, "measure", "id") or _find_col(cols, "measure")
        geo_col      = _find_col(cols, "geographic", "level")
        location_col = _find_col(cols, "location", "id") or _find_col(cols, "fips")

        print(f"[PLACES] measure_col={measure_col}, geo_col={geo_col}, "
              f"location_col={location_col}")

        measures_sql = ", ".join(f"'{m}'" for m in PLACES_MEASURES)
        where_parts = []
        if geo_col:
            where_parts.append(f"{geo_col}='County'")
        if measure_col:
            where_parts.append(f"{measure_col} in({measures_sql})")
        where = " AND ".join(where_parts) if where_parts else None

        print(f"[PLACES] WHERE clause: {where}")
        all_rows = _fetch_all_pages(session, url, where, location_col)

    df = pd.DataFrame(all_rows)
    print(f"[PLACES] Total rows downloaded: {len(df):,}")
    return df


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    """Select and rename columns needed by places_raw table."""
    # Normalise column names to lowercase for consistent matching
    df.columns = [c.lower() for c in df.columns]

    # Map whichever column names exist to our standard names
    col_candidates = {
        "fips":       ["locationid", "fips", "countyfips"],
        "state_abbr": ["stateabbr", "state_abbr", "state"],
        "county_name":["locationname", "county_name", "countyname"],
        "measure_id": ["measureid", "measure_id"],
        "data_value": ["data_value", "datavalue"],
        "low_ci":     ["low_confidence_limit", "lowconfidencelimit", "low_ci"],
        "high_ci":    ["high_confidence_limit", "highconfidencelimit", "high_ci"],
        "population": ["totalpopulation", "total_population", "population"],
        "year":       ["year", "datayear"],
    }

    rename = {}
    for target, candidates in col_candidates.items():
        for c in candidates:
            if c in df.columns:
                rename[c] = target
                break

    df = df[list(rename.keys())].rename(columns=rename)

    df["fips"]       = df["fips"].astype(str).str.zfill(5)
    df["data_value"] = pd.to_numeric(df["data_value"], errors="coerce")
    for opt in ["low_ci", "high_ci", "population", "year"]:
        if opt in df.columns:
            df[opt] = pd.to_numeric(df[opt], errors="coerce")

    # Keep only target measures (in case geo filter wasn't applied)
    if "measure_id" in df.columns:
        df = df[df["measure_id"].isin(PLACES_MEASURES)]

    return df.drop_duplicates(subset=["fips", "measure_id"])


def run(save_raw: bool = True) -> pd.DataFrame:
    init_db()
    raw = fetch_places_data()

    if save_raw:
        raw_path = RAW_DIR / "places_raw.csv"
        raw.to_csv(raw_path, index=False)
        print(f"[PLACES] Raw CSV saved → {raw_path}")

    normalised = _normalise(raw)
    print(f"[PLACES] Measures found: {normalised['measure_id'].unique() if 'measure_id' in normalised.columns else 'unknown'}")
    upsert_dataframe(normalised, "places_raw", if_exists="replace")
    return normalised


if __name__ == "__main__":
    run()
