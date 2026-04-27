"""
Apache Airflow DAG — Urban Health & Park Access Pipeline
=========================================================

Task graph:
                   ┌─ ingest_places ─ clean_places ─┐
  init_database ──►├─ ingest_chr    ─ clean_chr    ─ ┼─► merge_transform ─► done
                   └─ ingest_urban  ─ clean_urban  ─┘

Every task writes its output to the SQLite database so that downstream tasks
and Tableau can read directly from the DB — no CSV hand-offs.

Schedule: weekly on Sunday at 02:00 UTC.

Setup:
  1. Set the environment variable HEALTH_PIPELINE_HOME to the project root, e.g.:
       export HEALTH_PIPELINE_HOME=/path/to/health_pipeline   (Linux/Mac)
       set HEALTH_PIPELINE_HOME=C:\\path\\to\\health_pipeline  (Windows)
     Or add it to airflow.cfg under [core] → default_env_vars.
  2. Copy (or symlink) this file into $AIRFLOW_HOME/dags/
  3. Enable the DAG in the Airflow UI.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# ── Resolve project root ──────────────────────────────────────────────────────
# Prefer the explicit env var; fall back to the directory above this file.
_PROJECT_ROOT = Path(
    os.environ.get("HEALTH_PIPELINE_HOME", Path(__file__).parent.parent)
)
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

from database.db_utils           import init_db
from scripts.ingest_places       import run as ingest_places
from scripts.ingest_chr          import run as ingest_chr
from scripts.ingest_urban_rural  import run as ingest_urban
from scripts.ingest_places_csv   import run as ingest_places_csv
from scripts.clean_places        import clean_places
from scripts.clean_chr           import clean_chr
from scripts.clean_urban_rural   import clean_urban_rural
from scripts.merge_transform     import run as merge_transform

# ── Default arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner":            "csds397",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="urban_health_park_pipeline",
    description="Ingest CDC PLACES + CHR + NCHS → SQLite → county_analysis (Tableau source)",
    default_args=default_args,
    schedule="0 2 * * 0",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["health", "csds397"],
    doc_md=__doc__,
) as dag:

    t_init = PythonOperator(
        task_id="init_database",
        python_callable=init_db,
        doc_md="Create all SQLite tables defined in schema.sql (idempotent).",
    )

    # ── Ingestion: pull from external sources → raw DB tables ─────────────────
    t_ingest_places = PythonOperator(
        task_id="ingest_places",
        python_callable=ingest_places,
        doc_md="CDC PLACES API → places_raw table.",
    )
    t_ingest_chr = PythonOperator(
        task_id="ingest_chr",
        python_callable=ingest_chr,
        doc_md="County Health Rankings CSV → chr_raw table.",
    )
    t_ingest_urban = PythonOperator(
        task_id="ingest_urban_rural",
        python_callable=ingest_urban,
        doc_md="NCHS Urban-Rural Excel → urban_rural table.",
    )

    t_ingest_places_csv = PythonOperator(
        task_id="ingest_places_csv",
        python_callable=ingest_places_csv,
        doc_md="Supplement API data with data/raw/places_full.csv (KY, PA). No-op if file absent.",
    )

    # ── Cleaning: validate + normalise → clean DB tables ─────────────────────
    t_clean_places = PythonOperator(
        task_id="clean_places",
        python_callable=clean_places,
        doc_md="Pivot, dedup, range-filter places_raw → places_clean table.",
    )
    t_clean_chr = PythonOperator(
        task_id="clean_chr",
        python_callable=clean_chr,
        doc_md="Range-filter + state-median impute chr_raw → chr_clean table.",
    )
    t_clean_urban = PythonOperator(
        task_id="clean_urban_rural",
        python_callable=clean_urban_rural,
        doc_md="Validate codes, drop territories, overwrite urban_rural table.",
    )

    # ── Transform: join + feature engineering → county_analysis table ────────
    t_merge = PythonOperator(
        task_id="merge_transform",
        python_callable=merge_transform,
        doc_md=(
            "Inner-join places_clean × chr_clean, left-join urban_rural. "
            "Engineer inactivity_zscore, le_deficit, park_quartile, ur_label. "
            "Write final county_analysis table — this is the Tableau data source."
        ),
    )

    t_done = PythonOperator(
        task_id="pipeline_complete",
        python_callable=lambda: print(
            "[DAG] Pipeline complete. "
            "Tableau source: county_analysis / v_county_analysis view in health_pipeline.db"
        ),
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    t_init >> [t_ingest_places, t_ingest_chr, t_ingest_urban]

    t_ingest_places >> t_ingest_places_csv >> t_clean_places
    t_ingest_chr    >> t_clean_chr
    t_ingest_urban  >> t_clean_urban

    [t_clean_places, t_clean_chr, t_clean_urban] >> t_merge >> t_done


if __name__ == "__main__":
    dag.test()
