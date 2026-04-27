# Urban Health & Park Access — Data Pipeline

CSDS 397 Final Project

## What This Uses

- **Astronomer (Astro)** — managed Apache Airflow cloud platform; hosts the DAG, scheduler, and public Airflow UI
- **Apache Airflow** — orchestrates and schedules the pipeline; DAG definition lives in `dags/`
- **Neon** — free cloud PostgreSQL database; the pipeline writes here and Tableau reads from here
- **GitHub Actions** — automatically deploys updated DAG code to Astronomer on every push to `main`
- **Tableau Desktop** — connects directly to Neon via the built-in PostgreSQL connector

## Prerequisites

```bash
pip install -r requirements.txt
```

The following data files are already included in the repository under `data/raw/`:

| File | Source |
|------|--------|
| `places_full.csv` | CDC PLACES — full county-level CSV |
| `chr_2024_raw.csv` | County Health Rankings 2024 analytic data |
| `nchs_urban_rural.xlsx` | NCHS Urban-Rural Classification 2013 |

## Running Locally

```bash
python run_pipeline.py
```

By default this uses a local SQLite file. To run against Neon instead, set `DATABASE_URL` first:

```powershell
$env:DATABASE_URL = "postgresql://user:password@host/dbname?sslmode=require"
python run_pipeline.py
```

Partial runs:

```bash
python run_pipeline.py --step 5        # resume from step 5
python run_pipeline.py --only 8        # run only merge_transform
python run_pipeline.py --dry           # print steps without running
```

Steps: `1 init_db → 2 ingest_places_csv → 3 ingest_chr → 4 ingest_urban_rural → 5 clean_places → 6 clean_chr → 7 clean_urban_rural → 8 merge_transform → 9 run_analysis`

## Cloud Setup (Astronomer + Neon)

### Neon (database)

1. Create a free project at [neon.tech](https://neon.tech) and copy the connection string

### Astronomer (Airflow)

1. Create a free account at [astronomer.io](https://astronomer.io) and create a Deployment
2. Connect your GitHub repo under **Git** settings — set branch to `main`
3. In your Deployment → add environment variable `DATABASE_URL` set to your Neon connection string
4. In your GitHub repo: **Settings → Secrets → Actions** — add:
   - `ASTRONOMER_DEPLOYMENT_ID` — from your Astronomer deployment page
   - `ASTRONOMER_API_TOKEN` — from Deployment → Access → API Tokens

Every push to `main` automatically deploys updated code to Astronomer via GitHub Actions. Trigger a manual DAG run from the Astronomer Airflow UI.

## Connecting Tableau

1. Tableau Desktop → **Connect → PostgreSQL**
2. Enter your Neon host, port `5432`, database name, username, and password
3. Check **Require SSL**
4. Drag **`v_county_analysis`** onto the canvas — this view has human-readable column names

## Project Structure

```
health_pipeline/
├── config.py
├── run_pipeline.py
├── Dockerfile                   ← Astro runtime image definition
├── packages.txt                 ← System packages for Astro build
├── dags/health_pipeline_dag.py  ← Airflow DAG
├── .astro/config.yaml           ← Astronomer project config
├── .github/workflows/
│   ├── pipeline.yml             ← GitHub Actions: run pipeline (local/backup)
│   └── astronomer_deploy.yml    ← GitHub Actions: deploy to Astronomer
├── scripts/
│   ├── ingest_places_csv.py
│   ├── ingest_chr.py
│   ├── ingest_urban_rural.py
│   ├── clean_places.py
│   ├── clean_chr.py
│   ├── clean_urban_rural.py
│   └── merge_transform.py
├── database/
│   ├── schema.sql
│   └── db_utils.py
├── analysis/
│   └── hypotheses.py
└── data/
    ├── raw/
    └── processed/
```
