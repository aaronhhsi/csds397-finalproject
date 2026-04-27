import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
DB_PATH = BASE_DIR / "health_pipeline.db"

# ── Database ──────────────────────────────────────────────────────────────────
# Set DATABASE_URL to use a cloud PostgreSQL database (e.g. Neon).
# If unset, the pipeline falls back to local SQLite (DB_PATH above).
#
# Neon connection string format:
#   postgresql://user:password@ep-xxx.us-east-2.aws.neon.tech/dbname?sslmode=require
#
# Set via environment variable (never hard-code credentials):
#   export DATABASE_URL="postgresql://..."        # Linux/Mac
#   set DATABASE_URL=postgresql://...             # Windows CMD
#   $env:DATABASE_URL="postgresql://..."          # PowerShell
DATABASE_URL: str | None = os.environ.get("DATABASE_URL", None)

# ── CDC PLACES (Socrata API) ──────────────────────────────────────────────────
# County-level 2024 release (based on 2022 BRFSS data).
# If Socrata returns a 404, visit https://data.cdc.gov/browse and search
# "PLACES County" to find the current dataset ID.
PLACES_BASE_URL = "https://data.cdc.gov/resource"
PLACES_DATASET_ID = "swc5-untb"   # county-level release
PLACES_APP_TOKEN = ""                    # Optional: register at data.cdc.gov for higher rate limits
PLACES_MEASURES = ["LPA", "OBESITY", "CSMOKING"]   # inactivity, obesity, smoking
PLACES_PAGE_SIZE = 50_000               # Socrata hard cap is 50 000 per request

# ── County Health Rankings (CSV) ─────────────────────────────────────────────
# One wide CSV per year; column mapping may shift slightly between years.
# Verify at https://www.countyhealthrankings.org/health-data/methodology-and-sources/data-documentation
CHR_URL = (
    "https://www.countyhealthrankings.org/sites/default/files/media/document/"
    "analytic_data2024.csv"
)
CHR_YEAR = 2024

# Known measure column prefixes (raw values) for CHR 2024.
# Update these if CHR changes numbering; run scripts/ingest_chr.py --list-cols
# to see all column names in the downloaded file.
CHR_COL_PARK_ACCESS   = "v011_rawvalue"   # % with access to exercise opportunities
CHR_COL_INACTIVITY    = "v070_rawvalue"   # % physically inactive (cross-check w/ PLACES)
CHR_COL_OBESITY       = "v011_rawvalue"   # placeholder — see note below
CHR_COL_SMOKING       = "v044_rawvalue"   # % adult smokers
CHR_COL_LIFE_EXP      = "v147_rawvalue"   # life expectancy (years)

# NOTE: CHR col numbers for adult obesity changed in 2022; run --list-cols to confirm.
# The primary obesity & inactivity source for this project is CDC PLACES.

# ── NCHS Urban-Rural Classification ─────────────────────────────────────────
NCHS_URBAN_URL = "https://www.cdc.gov/nchs/data/data_acces_files/NCHSURCodes2013.xlsx"
# Codes: 1=Large central metro, 2=Large fringe metro, 3=Medium metro,
#        4=Small metro, 5=Micropolitan, 6=Non-core (rural)
URBAN_CODES = {1, 2, 3, 4}   # codes treated as "urban" for Hypothesis 1

# ── Analysis ─────────────────────────────────────────────────────────────────
ALPHA = 0.05   # significance threshold
RANDOM_SEED = 42
