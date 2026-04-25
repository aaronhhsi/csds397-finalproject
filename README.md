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
CDC PLACES API ──► ingest_places ──► clean_places ─┐                  ┌──► Tableau
CHR CSV        ──► ingest_chr    ──► clean_chr    ──┼──► merge_transform ──► SQLite DB ──► (ODBC)
NCHS Excel     ──► ingest_urban  ──► clean_urban  ─┘  county_analysis    └──► Dash app
                        │
                   Apache Airflow (orchestrates & schedules all tasks)
```

Every step writes to the SQLite database (`health_pipeline.db`).
Tableau connects directly to the DB via ODBC — no CSV imports.

---

## Cloud Setup (Neon + GitHub Actions)

### Step 1 — Create a free Neon database

1. Go to **[neon.tech](https://neon.tech)** → sign up (free, no credit card)
2. Create a new project → name it `health_pipeline`
3. On the dashboard, click **Connection string** → copy the string that looks like:
   ```
   postgresql://aaron:abc123@ep-cool-name-123.us-east-2.aws.neon.tech/neondb?sslmode=require
   ```

### Step 2 — Push the project to GitHub

```bash
cd health_pipeline
git init
git add .
git commit -m "Initial pipeline"
git remote add origin https://github.com/YOUR_USERNAME/health_pipeline.git
git push -u origin main
```

### Step 3 — Add the connection string as a GitHub Secret

In your GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**

| Name | Value |
|------|-------|
| `DATABASE_URL` | the Neon connection string from Step 1 |

### Step 4 — Run the pipeline

Go to **Actions → Health Pipeline → Run workflow** to trigger it manually.
It will also run automatically every Sunday at 02:00 UTC.

The pipeline writes directly to Neon — no local database needed.

### Step 5 — Connect Tableau to Neon

Neon is standard PostgreSQL, so Tableau connects natively — no ODBC driver needed.

1. Tableau Desktop → **Connect → PostgreSQL**
2. Fill in the fields from your Neon connection string:

| Tableau field | Neon value (example) |
|---------------|----------------------|
| Server | `ep-cool-name-123.us-east-2.aws.neon.tech` |
| Port | `5432` |
| Database | `neondb` |
| Username | `aaron` |
| Password | your Neon password |

3. Check **Require SSL**
4. Click **Sign In** → drag **`v_county_analysis`** onto the canvas

---

## Local Development (SQLite fallback)

If `DATABASE_URL` is not set, the pipeline uses a local SQLite file automatically.
No configuration needed — just run:

```bash
python run_pipeline.py
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

## Tableau Connection (Direct DB — No CSV)

Tableau connects to `health_pipeline.db` using the SQLite ODBC driver.

### Step 1 — Install the SQLite ODBC driver (Windows)

Download and install the 64-bit driver from:
**http://www.ch-werner.de/sqliteodbc/** → `sqliteodbc_w64.exe`

### Step 2 — Connect Tableau Desktop

1. Open Tableau Desktop → **Connect** → **More…** → **Other Databases (ODBC)**
2. Select **SQLite3 ODBC Driver** from the DSN/Driver list
3. Click **Connect** → in the connection string box enter:
   ```
   Database=C:\Users\aaron\Documents\CSDS397\health_pipeline\health_pipeline.db
   ```
4. Click **Sign In**

### Step 3 — Select the data source table

In the Data Source pane, drag the **`v_county_analysis`** view onto the canvas.
This view has human-readable column names (e.g. `"Physical Inactivity (%)"`,
`"Life Expectancy (years)"`) and covers all variables needed for the visualisations.

The underlying `county_analysis` table is also available if you need raw column names.

### Recommended Tableau visualisations

| Sheet | Chart type | X axis | Y axis | Color/Size |
|-------|-----------|--------|--------|------------|
| H1 — Park vs Inactivity | Scatter | Park Access (%) | Physical Inactivity (%) | State (urban filter) |
| H2 — Inactivity vs Life Exp | Scatter | Physical Inactivity (%) | Life Expectancy (years) | Smoking % (color), Obesity % (size) |
| National Map | Filled map | FIPS / County | — | Physical Inactivity (%) |
| Urban-Rural Breakdown | Box plot / Bar | Urban-Rural Label | Life Expectancy (years) | — |
| Correlation overview | Heatmap (manual) | Variable | Variable | Correlation value |

---

## Airflow Setup

```bash
# 1. Tell the DAG where your project lives
export HEALTH_PIPELINE_HOME=/path/to/health_pipeline   # Linux/Mac
# set HEALTH_PIPELINE_HOME=C:\path\to\health_pipeline  # Windows (CMD)

# 2. Copy the DAG file into Airflow's DAG folder
cp dags/health_pipeline_dag.py $AIRFLOW_HOME/dags/

# 3. Start Airflow
airflow scheduler &
airflow webserver --port 8080
```

Visit `http://localhost:8080` → enable `urban_health_park_pipeline`.

The DAG writes every stage to `health_pipeline.db`, so Tableau always reads
the freshest data after any scheduled or manual run.

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
