"""
One-off debug script: load places_full.csv into Neon as-is (no filtering)
so you can run SQL queries to diagnose what's being dropped in ingestion.

Usage:
    $env:DATABASE_URL = "postgresql://..."   # PowerShell
    python scripts/upload_places_debug.py

Creates table: places_full_debug  (dropped and recreated each run)
"""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import RAW_DIR, DATABASE_URL

CSV_PATH = RAW_DIR / "places_full.csv"


def run():
    if not DATABASE_URL:
        raise RuntimeError("Set DATABASE_URL environment variable first.")
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"{CSV_PATH} not found.")

    print(f"[debug] Reading {CSV_PATH} …")
    df = pd.read_csv(CSV_PATH, dtype=str, low_memory=False)
    print(f"[debug] {len(df):,} rows, {len(df.columns)} columns")

    # Lowercase column names so SQL queries are predictable
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    print(f"[debug] Columns: {list(df.columns)}")

    engine = create_engine(DATABASE_URL, echo=False)
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS places_full_debug"))

    df.to_sql("places_full_debug", engine, if_exists="replace", index=False)
    print(f"[debug] Uploaded {len(df):,} rows → places_full_debug in Neon")
    print("[debug] Now run the diagnostic SQL queries in the Neon SQL editor.")


if __name__ == "__main__":
    run()
