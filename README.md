# Urban Health & Park Access — Data Pipeline

CSDS 397 Final Project

## Research Hypotheses

| # | Statement |
|---|-----------|
| H1 | In urban U.S. counties, higher park/exercise access is associated with **lower** physical inactivity rates. |
| H2 | Higher inactivity rates are associated with **lower** life expectancy, even after controlling for obesity and smoking. |

---

## Data Sources

| Source | What it provides | Format | Access |
|--------|-----------------|--------|--------|
| [CDC PLACES 2024](https://www.cdc.gov/places/) | Inactivity %, obesity %, smoking % (county-level, BRFSS-derived) | Socrata REST API | Free, no key needed |
| [County Health Rankings 2024](https://www.countyhealthrankings.org) | Park/exercise access %, life expectancy | CSV download | Free |
| [NCHS Urban-Rural 2013](https://www.cdc.gov/nchs/data_access/urban_rural.htm) | Urban-rural classification codes 1–6 | Excel (CDC FTP) | Free |

---

## Architecture

```
CDC PLACES API ──► ingest_places ──► clean_places ─┐
CHR CSV        ──► ingest_chr    ──► clean_chr    ──┼──► merge_transform ──► SQLite DB ──► Dashboard
NCHS Excel     ──► ingest_urban  ──► clean_urban  ─┘
                        │
                   Apache Airflow (schedules & monitors all tasks)
```

---

## Quick Start

### 1. Install dependencies

```bash
cd health_pipeline
pip install -r requirements.txt
```

> **Airflow** requires a separate install (Linux/Mac only for native install):
> ```bash
> pip install apache-airflow==2.10.2
> airflow db init
> airflow users create --username admin --password admin \
>     --firstname A --lastname B --role Admin --email a@b.com
> ```
> On Windows, use WSL2 or Docker. See [Airflow docs](https://airflow.apache.org/docs/apache-airflow/stable/installation/).

### 2. Run the pipeline (without Airflow)

```bash
python run_pipeline.py
```

To start from a specific step (e.g. skip re-downloading):

```bash
python run_pipeline.py --step 5    # start at clean_places
python run_pipeline.py --only 8   # run only merge_transform
python run_pipeline.py --dry      # print steps without running
```

### 3. Launch the dashboard

```bash
python dashboard/app.py
```

Open [http://127.0.0.1:8050](http://127.0.0.1:8050).

### 4. Run statistical analysis only

```bash
python analysis/hypotheses.py
```

---

## Airflow Setup

Copy the DAG to Airflow's DAG folder, then update the import path:

```bash
# Find your Airflow home
echo $AIRFLOW_HOME   # typically ~/airflow

cp dags/health_pipeline_dag.py $AIRFLOW_HOME/dags/

# Start the scheduler and webserver
airflow scheduler &
airflow webserver --port 8080
```

Visit `http://localhost:8080` → enable `urban_health_park_pipeline`.

---

## CHR Column Mapping

County Health Rankings column numbers can shift between years. If `clean_chr`
logs warnings about missing columns, run:

```bash
python scripts/ingest_chr.py --list-cols
```

Then update `config.py` with the correct column names for each measure.

---

## Project Structure

```
health_pipeline/
├── config.py               ← All configuration constants
├── requirements.txt
├── run_pipeline.py         ← Run full pipeline locally
├── dags/
│   └── health_pipeline_dag.py   ← Airflow DAG
├── scripts/
│   ├── ingest_places.py    ← CDC PLACES via Socrata API
│   ├── ingest_chr.py       ← County Health Rankings CSV
│   ├── ingest_urban_rural.py  ← NCHS Urban-Rural Excel
│   ├── clean_places.py
│   ├── clean_chr.py
│   ├── clean_urban_rural.py
│   └── merge_transform.py  ← Join + feature engineering
├── database/
│   ├── schema.sql          ← Table definitions
│   └── db_utils.py         ← SQLAlchemy helpers
├── analysis/
│   └── hypotheses.py       ← Spearman correlation, OLS, partial correlation
├── dashboard/
│   └── app.py              ← Plotly Dash interactive dashboard
└── data/
    ├── raw/                ← Downloaded files (gitignored)
    └── processed/          ← Cleaned CSVs (gitignored)
```
