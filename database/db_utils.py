"""Thin helpers around SQLAlchemy / SQLite."""

import sqlite3
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH

_SCHEMA = Path(__file__).parent / "schema.sql"


def get_engine(db_path: Path = DB_PATH):
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(db_path: Path = DB_PATH) -> None:
    """Create all tables defined in schema.sql (idempotent)."""
    engine = get_engine(db_path)
    schema_sql = _SCHEMA.read_text()
    with engine.begin() as conn:
        for statement in schema_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))
    print(f"[db] Initialised database at {db_path}")


def upsert_dataframe(df: pd.DataFrame, table: str, db_path: Path = DB_PATH,
                     if_exists: str = "append") -> None:
    """Write a DataFrame to a table, replacing existing rows when possible."""
    engine = get_engine(db_path)
    df.to_sql(table, engine, if_exists=if_exists, index=False)
    print(f"[db] Wrote {len(df):,} rows → {table}")


def read_table(table: str, db_path: Path = DB_PATH) -> pd.DataFrame:
    engine = get_engine(db_path)
    return pd.read_sql_table(table, engine)


def query(sql: str, db_path: Path = DB_PATH, params: dict = None) -> pd.DataFrame:
    engine = get_engine(db_path)
    with engine.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})
