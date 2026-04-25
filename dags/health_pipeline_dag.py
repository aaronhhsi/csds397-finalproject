"""
Apache Airflow DAG — Urban Health & Park Access Pipeline
=========================================================

Task graph:
                   ┌─ ingest_places ─ clean_places ─┐
  init_database ──►├─ ingest_chr    ─ clean_chr    ─ ┼─► merge_transform ─► done
                   └─ ingest_urban  ─ clean_urban  ─┘

Schedule: weekly on Sunday at 02:00 UTC (data is updated annually, so this
          mostly serves as a freshness check / no-op if source hasn't changed).

Running locally (outside Airflow UI):
    python dags/health_pipeline_dag.py   (calls dag.test() for a dry run)
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Allow imports from project root when running standalone
sys.path.insert(0, str(Path(__file__).parent.parent))

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Callables imported from scripts ──────────────────────────────────────────
from database.db_utils import init_db
from scripts.ingest_places      import run as ingest_places
from scripts.ingest_chr         import run as ingest_chr
from scripts.ingest_urban_rural import run as ingest_urban
from scripts.clean_places       import clean_places
from scripts.clean_chr          import clean_chr
from scripts.clean_urban_rural  import clean_urban_rural
from scripts.merge_transform    import run as merge_transform

# ── DAG default arguments ─────────────────────────────────────────────────────
default_args = {
    "owner":            "csds397",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

# ── DAG definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="urban_health_park_pipeline",
    description="Ingest, clean, and merge CDC PLACES + CHR + NCHS urban-rural data",
    default_args=default_args,
    schedule="0 2 * * 0",          # every Sunday at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["health", "csds397"],
    doc_md=__doc__,
) as dag:

    t_init = PythonOperator(
        task_id="init_database",
        python_callable=init_db,
        doc_md="Create SQLite tables defined in database/schema.sql (idempotent).",
    )

    # ── Ingestion ─────────────────────────────────────────────────────────────
    t_ingest_places = PythonOperator(
        task_id="ingest_places",
        python_callable=ingest_places,
        doc_md="Download CDC PLACES county data (LPA, OBESITY, CSMOKING) via Socrata API.",
    )

    t_ingest_chr = PythonOperator(
        task_id="ingest_chr",
        python_callable=ingest_chr,
        doc_md="Download County Health Rankings CSV (park access, life expectancy).",
    )

    t_ingest_urban = PythonOperator(
        task_id="ingest_urban_rural",
        python_callable=ingest_urban,
        doc_md="Download NCHS 2013 Urban-Rural Classification Excel from CDC FTP.",
    )

    # ── Cleaning ──────────────────────────────────────────────────────────────
    t_clean_places = PythonOperator(
        task_id="clean_places",
        python_callable=clean_places,
        doc_md="Pivot, deduplicate, and range-filter PLACES data.",
    )

    t_clean_chr = PythonOperator(
        task_id="clean_chr",
        python_callable=clean_chr,
        doc_md="Range-filter and state-median-impute CHR data.",
    )

    t_clean_urban = PythonOperator(
        task_id="clean_urban_rural",
        python_callable=clean_urban_rural,
        doc_md="Validate NCHS urban-rural codes; exclude territories.",
    )

    # ── Transform & load ──────────────────────────────────────────────────────
    t_merge = PythonOperator(
        task_id="merge_transform",
        python_callable=merge_transform,
        doc_md=(
            "Inner-join all three sources on FIPS, engineer derived features, "
            "and write county_analysis table."
        ),
    )

    t_done = PythonOperator(
        task_id="done",
        python_callable=lambda: print("[DAG] Pipeline finished successfully."),
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    t_init >> [t_ingest_places, t_ingest_chr, t_ingest_urban]

    t_ingest_places >> t_clean_places
    t_ingest_chr    >> t_clean_chr
    t_ingest_urban  >> t_clean_urban

    [t_clean_places, t_clean_chr, t_clean_urban] >> t_merge >> t_done


# ── Standalone dry-run ────────────────────────────────────────────────────────
if __name__ == "__main__":
    dag.test()
