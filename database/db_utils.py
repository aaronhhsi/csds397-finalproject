"""Thin helpers around SQLAlchemy — works with both SQLite (local) and PostgreSQL (Neon)."""

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import DB_PATH, DATABASE_URL

_SCHEMA = Path(__file__).parent / "schema.sql"


def get_engine(db_path: Path = DB_PATH):
    if DATABASE_URL:
        return create_engine(DATABASE_URL, echo=False)
    return create_engine(f"sqlite:///{db_path}", echo=False)


def init_db(db_path: Path = DB_PATH) -> None:
    """Create all tables and views (idempotent). Handles SQLite and PostgreSQL dialects."""
    engine = get_engine(db_path)
    schema_sql = _SCHEMA.read_text()

    if engine.dialect.name == "postgresql":
        # Adapt SQLite-specific syntax for PostgreSQL
        schema_sql = schema_sql.replace("AUTOINCREMENT", "")
        schema_sql = schema_sql.replace("datetime('now')", "CURRENT_TIMESTAMP")
        # PostgreSQL uses CREATE OR REPLACE VIEW instead of CREATE VIEW IF NOT EXISTS
        schema_sql = schema_sql.replace(
            "CREATE VIEW IF NOT EXISTS", "CREATE OR REPLACE VIEW"
        )

    with engine.begin() as conn:
        for statement in schema_sql.split(";"):
            stmt = statement.strip()
            if stmt:
                try:
                    conn.execute(text(stmt))
                except Exception as e:
                    # Log but don't abort — some statements may already exist
                    print(f"[db] Schema note: {e}")

    db_label = DATABASE_URL[:40] + "…" if DATABASE_URL else str(db_path)
    print(f"[db] Initialised database: {db_label}")


def upsert_dataframe(df: pd.DataFrame, table: str, db_path: Path = DB_PATH,
                     if_exists: str = "append") -> None:
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
