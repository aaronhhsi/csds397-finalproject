-- ──────────────────────────────────────────────────────────────────────────
-- health_pipeline database schema
-- All monetary / percentage values stored as REAL (floating point).
-- FIPS codes stored as TEXT with leading zeros preserved (e.g. "01001").
-- ──────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS places_raw (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fips            TEXT    NOT NULL,
    state_abbr      TEXT,
    county_name     TEXT,
    measure_id      TEXT    NOT NULL,   -- LPA | OBESITY | CSMOKING
    data_value      REAL,
    low_ci          REAL,
    high_ci         REAL,
    population      INTEGER,
    year            INTEGER,
    ingested_at     TEXT    DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS chr_raw (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    fips            TEXT    NOT NULL,
    state_abbr      TEXT,
    county_name     TEXT,
    park_access_pct REAL,              -- v011_rawvalue (0–1 fraction)
    life_expectancy REAL,              -- v147_rawvalue (years)
    inactivity_chk  REAL,              -- v070_rawvalue (cross-check, 0–1)
    smoking_chk     REAL,              -- v044_rawvalue (cross-check, 0–1)
    year            INTEGER,
    ingested_at     TEXT    DEFAULT (CURRENT_TIMESTAMP)
);

CREATE TABLE IF NOT EXISTS urban_rural (
    fips            TEXT    PRIMARY KEY,
    state_abbr      TEXT,
    county_name     TEXT,
    ur_code         INTEGER,           -- 1–6 NCHS classification
    is_urban        INTEGER            -- 1 = urban (ur_code in URBAN_CODES)
);

CREATE TABLE IF NOT EXISTS places_clean (
    fips              TEXT    PRIMARY KEY,
    state_abbr        TEXT,
    county_name       TEXT,
    inactivity_rate   REAL,
    obesity_rate      REAL,
    smoking_rate      REAL
);

CREATE TABLE IF NOT EXISTS chr_clean (
    fips              TEXT    PRIMARY KEY,
    state_abbr        TEXT,
    county_name       TEXT,
    park_access_pct   REAL,
    life_expectancy   REAL,
    inactivity_chk    REAL,
    smoking_chk       REAL,
    year              INTEGER
);

CREATE TABLE IF NOT EXISTS county_analysis (
    fips              TEXT    PRIMARY KEY,
    state_abbr        TEXT    NOT NULL,
    county_name       TEXT    NOT NULL,
    ur_code           INTEGER,
    is_urban          INTEGER,
    park_access_pct   REAL,            -- % with access to exercise locations (0–100)
    inactivity_rate   REAL,            -- % physically inactive (0–100)
    obesity_rate      REAL,            -- % obese adults (0–100)
    smoking_rate      REAL,            -- % current smokers (0–100)
    life_expectancy   REAL,            -- years
    created_at        TEXT    DEFAULT (CURRENT_TIMESTAMP)
);

-- Tableau-facing view: clean labels, all analysis columns in one place
CREATE VIEW IF NOT EXISTS v_county_analysis AS
    SELECT
        ca.fips,
        ca.state_abbr        AS "State",
        ca.county_name       AS "County",
        ca.ur_code           AS "Urban-Rural Code",
        CASE ca.ur_code
            WHEN 1 THEN 'Large central metro'
            WHEN 2 THEN 'Large fringe metro'
            WHEN 3 THEN 'Medium metro'
            WHEN 4 THEN 'Small metro'
            WHEN 5 THEN 'Micropolitan'
            WHEN 6 THEN 'Non-core (rural)'
            ELSE 'Unknown'
        END                  AS "Urban-Rural Label",
        ca.is_urban          AS "Is Urban",
        ca.park_access_pct   AS "Park Access (%)",
        ca.inactivity_rate   AS "Physical Inactivity (%)",
        ca.obesity_rate      AS "Obesity (%)",
        ca.smoking_rate      AS "Smoking (%)",
        ca.life_expectancy   AS "Life Expectancy (years)",
        ca.inactivity_zscore AS "Inactivity Z-Score",
        ca.le_deficit        AS "Life Expectancy Deficit",
        ca.park_quartile     AS "Park Access Quartile"
    FROM county_analysis ca
    WHERE ca.fips IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_places_raw_fips       ON places_raw(fips);
CREATE INDEX IF NOT EXISTS idx_places_raw_measure    ON places_raw(measure_id);
CREATE INDEX IF NOT EXISTS idx_chr_raw_fips          ON chr_raw(fips);
CREATE INDEX IF NOT EXISTS idx_analysis_state        ON county_analysis(state_abbr);
CREATE INDEX IF NOT EXISTS idx_analysis_urban        ON county_analysis(is_urban);
