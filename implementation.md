# Implementation Guide: E-Commerce Analytics & Operations Platform (Snowflake + SiS)

> **Purpose:** This is your complete, step-by-step, copy-paste-ready engineering guide to build the entire **Snowflake + Streamlit in Snowflake (SiS)** e-commerce analytics platform yourself—from an empty folder to automated GitHub Actions CI/CD and documented SQL micro-partition pruning benchmarks.
> **How to use this file:** Follow every step **in order**. Every file you need to create has its **full, runnable source code** inline—no hidden snippets or placeholders. Do **not** move to the next phase until every check in the current phase's `[VERIFY]` gate passes.

---

## Legend & Tags

* `[LOCAL TERMINAL]` — Run these bash commands on your Linux workstation inside the project root.
* `[CREATE FILE]` — Create the exact file at the exact path with the full contents shown.
* `[SNOWSIGHT UI]` — Execute inside your Snowflake web console (Snowsight SQL Worksheet / UI).
* `[GITHUB UI]` — Configure inside your GitHub repository settings in the browser.
* `[VERIFY]` — Mandatory check you must pass before advancing.

---

## 0. Non-Negotiable Rules & Guardrails

1. **Zero Secrets in Git:** Never commit `.p8` private keys, passphrases, passwords, `.env`, `.streamlit/secrets.toml`, or Snowflake account identifiers in plaintext. Always use environment variables, local untracked files, or GitHub Secrets.
2. **Environment Isolation:** Never deploy experimental SQL or Streamlit changes directly to `ECOMM_PROD`. All development targets `ECOMM_DEV` via the `dev` branch first.
3. **No OLTP Anti-Patterns:** Never write single-row `INSERT` or `UPDATE` loops against Snowflake. Always use staged bulk loads (`COPY INTO`) or Snowpark DataFrame batch writes.
4. **Parameterized Queries Only:** All user-filtered SQL in Streamlit must use bound parameters (`?` placeholders with `params=[...]`) or Snowpark DataFrame `.filter()`—never f-string SQL interpolation.
5. **Cost Safety:** Every Snowflake virtual warehouse must specify `AUTO_SUSPEND = 60` and `AUTO_RESUME = TRUE`. Never leave automatic reclustering running on small dimension tables or idle trial tables.
6. **Runtime Portability:** Write the Streamlit session layer so the exact same Python app runs locally (`st.connection("snowflake")`) and inside SiS (`get_active_session()`).
7. **Verification Gate:** Never skip a `[VERIFY]` gate.

---

## 1. Architecture Overview

```text
[Local Dev / Feature Branch]
         |
         v (push to `dev` or PR merge to `main`)
[GitHub Actions CI/CD] --(ruff + pytest + RSA Key-Pair Auth)--> [Snowflake CLI (`snow`)]
                                                                       |
              +--------------------------------------------------------+
              |                                                        |
              v                                                        v
   [SQL DDL & Ingestion]                                    [Streamlit in Snowflake]
   - Warehouses (ETL vs App)                                - Container Runtime (`pyproject.toml`)
   - RBAC Roles & Grants                                    - Fallback (`environment.yml`)
   - Internal Stage -> COPY INTO -> RAW                     - `@st.cache_data(ttl=600)`
   - Snowpark Transforms -> MART                            - Reads ONLY from `MART` schema
   - Clustered `FACT_ORDERS` (Pruned Scans)
```

---

## 2. Target Repository Structure

By the end of Phase 5, your project folder will match this exact layout:

```text
.
├── plan.md                         # Master project specification (copied from Downloads)
├── implementation.md               # This step-by-step build guide
├── README.md                       # Recruiter-ready overview, setup, and screenshots
├── pyproject.toml                  # Python 3.11+ deps (Container Runtime + ruff, pytest, snow)
├── snowflake.yml                   # Snowflake CLI v2 project definition (dev/prod SiS deploy)
├── .env.example                    # Template for local connection env vars (never commit .env)
├── .gitignore                      # Ignores .env, *.p8, *.pub, secrets.toml, data/raw/, .venv/
├── .streamlit/
│   └── secrets.toml                # [LOCAL ONLY, gitignored] Local Snowflake creds for Streamlit
├── .github/
│   └── workflows/
│       ├── ci.yml                  # PR checks: ruff, pytest, SQL safety guardrails
│       ├── deploy-dev.yml          # Push to `dev` -> deploys SQL + SiS to ECOMM_DEV
│       └── deploy-prod.yml         # Merge to `main` -> deploys SQL + SiS to ECOMM_PROD
├── scripts/
│   ├── generate_data.py            # Vectorized synthetic e-commerce generator (50k to 5M+ rows)
│   └── run_snowpark_etl.py         # Snowpark Session API pipeline: RAW -> MART tables
├── sql/
│   ├── 01_bootstrap_account.sql    # [One-time ACCOUNTADMIN] Warehouses, DBs, Roles, Service User
│   ├── 02_schemas_and_stages.sql   # RAW, MART, APP schemas + internal stage + file formats
│   ├── 03_raw_tables.sql           # DDL for RAW landing tables
│   ├── 04_load_copy_into.sql       # Bulk COPY INTO statements with ON_ERROR handling
│   └── 05_mart_and_clustering.sql  # 5M-row benchmark + sorted load + clustering + result cache
├── app/
│   ├── streamlit_app.py            # Main SiS entry point (navigation & layout)
│   ├── environment.yml             # Warehouse Runtime compatibility manifest (fallback/reference)
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── snowflake_session.py    # Unified get_active_session() / local connection helper
│   │   └── queries.py              # Cached, parameterized queries against MART schema
│   └── views/
│       ├── __init__.py
│       ├── executive_kpi.py        # Revenue, Orders, AOV, Net Margin, daily trend
│       ├── cohort_retention.py     # Monthly customer retention heatmap
│       ├── inventory_alerts.py     # SKU velocity, stockout risk, top/bottom margin
│       └── order_lookup.py         # Parameterized order & customer search
├── tests/
│   ├── __init__.py
│   ├── test_data_generator.py      # Validates schema, FK integrity, and generator edge cases
│   └── test_queries.py             # Validates parameterized SQL, anti-injection, and KPI math
└── docs/
    ├── images/                     # Snowsight Query Profile screenshots & demo captures
    └── performance.md              # Before/after micro-partition pruning benchmark report
```

---

## 3. Local Environment Notes (Linux Workstation)

Your system `python3` is `3.10.12`, while this project targets **Python 3.11+**. You already have **`uv`** installed (`/home/admin1/.local/bin/uv`), which automatically downloads and manages a dedicated Python 3.11 virtual environment inside `.venv/` without altering your system Python.

Always activate `.venv` in your terminal before running `python`, `pytest`, `ruff`, `streamlit`, or `snow`:

```bash
source .venv/bin/activate
```

---

## Phase 0: Workspace, Account Setup & Project Scaffold

**Goal:** Set up your Snowflake trial account, GitHub repository, `dev`/`main` branch structure, Python 3.11 virtual environment, and linter/CLI toolchain.

### Step 0.1 `[SNOWSIGHT UI]` Create Snowflake Trial Account & Get Account Identifier

1. Sign up for a **free 30-day Snowflake trial** (Enterprise edition recommended so all features work; select AWS `us-east-1` or `us-west-2` if you want maximum SPCS Container Runtime availability).
2. Log into **Snowsight** (`https://app.snowflake.com`).
3. Open **Projects -> Worksheets -> + SQL Worksheet** and run:

```sql
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS ACCOUNT_IDENTIFIER;
```

4. Copy the exact result (e.g., `abc12345-xy67890`) into a scratch note outside this folder. **Never commit it to Git.**

---

### Step 0.2 `[GITHUB UI]` Create the Remote Repository

1. Go to GitHub and create a new **Public** repository named `snowflake-ecomm-platform`.
2. Leave **"Add a README"**, **".gitignore"**, and **"License"** unchecked so the repository starts completely empty.
3. Copy your HTTPS or SSH remote URL (e.g., `https://github.com/<your-username>/snowflake-ecomm-platform.git`).

---

### Step 0.3 `[LOCAL TERMINAL]` Initialize Git, Branches & Directory Tree

Open a terminal in `/home/admin1/Desktop/Self-learning-projects/Snowflake Ecommerce` and run:

```bash
cd "/home/admin1/Desktop/Self-learning-projects/Snowflake Ecommerce"

# 1. Initialize git with 'main' as default branch
git init -b main

# 2. Copy your reference plan into the project root
cp ~/Downloads/snowflake_streamlit_e_commerce_platform_plan.md ./plan.md

# 3. Create the full project directory skeleton
mkdir -p .github/workflows scripts sql app/utils app/views tests docs/images data/raw .streamlit

# 4. Create the 'dev' working branch and switch to it
git checkout -b dev
```

---

### Step 0.4 `[CREATE FILE]` Core Configuration Files

#### 1. Create `.gitignore`

```gitignore
# Environment & secrets (NEVER commit)
.env
.env.*
!.env.example
*.p8
*.pub
*.pem
.streamlit/secrets.toml

# Python & virtual environments
.venv/
venv/
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/
.pytest_cache/
.ruff_cache/

# Generated synthetic data & Snowflake local caches
data/raw/
*.parquet
*.csv
!docs/*.csv
.snowflake/

# OS & editor junk
.DS_Store
Thumbs.db
.vscode/
.idea/
```

#### 2. Create `.env.example`

```dotenv
# Copy this file to .env for local runs (NEVER commit .env)
SNOWFLAKE_ACCOUNT=orgname-accountname
SNOWFLAKE_USER=SVC_GITHUB_CICD
SNOWFLAKE_ROLE=ECOMM_CICD_ROLE
SNOWFLAKE_WAREHOUSE=ECOMM_ETL_WH
SNOWFLAKE_DATABASE=ECOMM_DEV
SNOWFLAKE_SCHEMA=MART
SNOWFLAKE_PRIVATE_KEY_PATH=/home/admin1/.snowflake/keys/ecomm_cicd_rsa_key.p8
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE=replace_with_your_passphrase
```

#### 3. Create `pyproject.toml`

```toml
[build-system]
requires = ["setuptools>=68.0"]
build-backend = "setuptools.build_meta"

[project]
name = "snowflake-ecomm-platform"
version = "0.1.0"
description = "End-to-end E-Commerce Analytics & Operations Platform on Snowflake and Streamlit in Snowflake"
readme = "README.md"
requires-python = ">=3.11"
dependencies = [
    "altair>=5.4.0",
    "numpy>=1.26.0",
    "pandas>=2.2.0",
    "pyarrow>=17.0.0",
    "python-dotenv>=1.0.1",
    "snowflake-snowpark-python>=1.22.0",
    "streamlit>=1.39.0",
]

[project.optional-dependencies]
dev = [
    "cryptography>=43.0.0",
    "pytest>=8.3.0",
    "ruff>=0.6.0",
    "snowflake-cli>=3.0.0",
]

[tool.setuptools.packages.find]
include = ["app*", "scripts*"]

[tool.ruff]
line-length = 100
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B"]
ignore = ["B008"]

[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
```

---

### Step 0.5 `[LOCAL TERMINAL]` Create Python 3.11 Environment & Install Tooling

```bash
# Provision Python 3.11 via uv and install project + dev dependencies
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e ".[dev]"
```

Connect your local repo to GitHub (replace `<your-username>`):

```bash
git remote add origin https://github.com/<your-username>/snowflake-ecomm-platform.git
```

---

### Step 0.6 `[VERIFY]` Phase 0 Definition of Done

Run all four checks in your terminal:

```bash
git branch
python --version
snow --version
ruff check .
```

**Expected output:**
* `git branch` shows `* dev` (and `main` once you make your first commit).
* `python --version` shows `Python 3.11.x`.
* `snow --version` prints the Snowflake CLI version (`3.x.x`).
* `ruff check .` exits with code `0` (no errors).

> **What you learned in Phase 0:**
> * Separating `dev` and `main` branches from day 1 mirrors real analytics engineering workflows where dev experiments never touch production.
> * `pyproject.toml` is the single source of truth for both local development and SiS Container Runtime dependencies.
> * `.gitignore` acts as your first line of defense against leaking `.p8` keys, `.env`, or local `secrets.toml`.

---
## Phase 1: Pillar 1 — Snowflake Basics, Governance, Ingestion & Snowpark ETL

**Goal:** Provision isolated warehouses, databases, least-privilege RBAC roles, and a service user in Snowflake; generate realistic synthetic e-commerce data; bulk-load it into `RAW` via `@ECOMM_STAGE` + `COPY INTO`; and transform `RAW` -> `MART` using the Snowpark Session API.

---

### Step 1.1 `[CREATE FILE]` `sql/01_bootstrap_account.sql`

This script runs **once** manually by `ACCOUNTADMIN` in Snowsight. CI/CD will **never** run `ACCOUNTADMIN` DDL.

```sql
-- =============================================================================
-- 01_bootstrap_account.sql
-- Purpose : One-time account bootstrap (Warehouses, DBs, RBAC, Service User)
-- Run As  : ACCOUNTADMIN (manually in Snowsight — NEVER in CI/CD)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 1. Virtual Warehouses (Workload Isolation + Cost Safety)
-- -----------------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS ECOMM_ETL_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for COPY INTO ingestion and Snowpark batch ETL';

CREATE WAREHOUSE IF NOT EXISTS ECOMM_APP_WH
    WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Dedicated warehouse for Streamlit in Snowflake analytical queries';

-- -----------------------------------------------------------------------------
-- 2. Environments (Databases & Schemas)
-- -----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS ECOMM_DEV;
CREATE DATABASE IF NOT EXISTS ECOMM_PROD;

CREATE SCHEMA IF NOT EXISTS ECOMM_DEV.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMM_DEV.MART;
CREATE SCHEMA IF NOT EXISTS ECOMM_DEV.APP;

CREATE SCHEMA IF NOT EXISTS ECOMM_PROD.RAW;
CREATE SCHEMA IF NOT EXISTS ECOMM_PROD.MART;
CREATE SCHEMA IF NOT EXISTS ECOMM_PROD.APP;

-- -----------------------------------------------------------------------------
-- 3. Least-Privilege Role Hierarchy
--    IMPORTANT: ECOMM_APP_ROLE and ECOMM_CICD_ROLE do NOT inherit from each
--    other. This guarantees the Streamlit app cannot read RAW tables.
-- -----------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ECOMM_CICD_ROLE
    COMMENT = 'CI/CD deployment role: owns RAW, MART, APP objects in DEV and PROD';

CREATE ROLE IF NOT EXISTS ECOMM_APP_ROLE
    COMMENT = 'Streamlit runtime role: SELECT on MART only; zero access to RAW';

CREATE ROLE IF NOT EXISTS ECOMM_VIEWER_ROLE
    COMMENT = 'Business viewer role: USAGE on Streamlit app in APP schema only';

GRANT ROLE ECOMM_CICD_ROLE TO ROLE SYSADMIN;
GRANT ROLE ECOMM_APP_ROLE TO ROLE SYSADMIN;
GRANT ROLE ECOMM_VIEWER_ROLE TO ROLE SYSADMIN;

-- -----------------------------------------------------------------------------
-- 4. Warehouse Grants
-- -----------------------------------------------------------------------------
GRANT USAGE, OPERATE ON WAREHOUSE ECOMM_ETL_WH TO ROLE ECOMM_CICD_ROLE;
GRANT USAGE ON WAREHOUSE ECOMM_APP_WH TO ROLE ECOMM_CICD_ROLE;
GRANT USAGE ON WAREHOUSE ECOMM_APP_WH TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON WAREHOUSE ECOMM_APP_WH TO ROLE ECOMM_VIEWER_ROLE;

-- -----------------------------------------------------------------------------
-- 5. ECOMM_DEV Grants
-- -----------------------------------------------------------------------------
-- CI/CD Role (Full build/deploy privileges)
GRANT USAGE, CREATE SCHEMA ON DATABASE ECOMM_DEV TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL FILE FORMATS IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA ECOMM_DEV.RAW TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_CICD_ROLE;

-- App Role (MART read-only + APP usage; ZERO RAW access)
GRANT USAGE ON DATABASE ECOMM_DEV TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECOMM_DEV.MART TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON ALL STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_APP_ROLE;

-- Viewer Role (Streamlit UI execution only)
GRANT USAGE ON DATABASE ECOMM_DEV TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON ALL STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA ECOMM_DEV.APP TO ROLE ECOMM_VIEWER_ROLE;

-- -----------------------------------------------------------------------------
-- 6. ECOMM_PROD Grants (Symmetric to DEV)
-- -----------------------------------------------------------------------------
GRANT USAGE, CREATE SCHEMA ON DATABASE ECOMM_PROD TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL STAGES IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STAGES IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL FILE FORMATS IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE FILE FORMATS IN SCHEMA ECOMM_PROD.RAW TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_CICD_ROLE;

GRANT ALL PRIVILEGES ON ALL STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_CICD_ROLE;
GRANT ALL PRIVILEGES ON FUTURE STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_CICD_ROLE;

GRANT USAGE ON DATABASE ECOMM_PROD TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_APP_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ECOMM_PROD.MART TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON ALL STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_APP_ROLE;
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_APP_ROLE;

GRANT USAGE ON DATABASE ECOMM_PROD TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON ALL STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_VIEWER_ROLE;
GRANT USAGE ON FUTURE STREAMLITS IN SCHEMA ECOMM_PROD.APP TO ROLE ECOMM_VIEWER_ROLE;

-- -----------------------------------------------------------------------------
-- 7. Optional SPCS Compute Pool (for Streamlit Container Runtime)
--    If your trial cloud/region does not support Snowpark Container Services,
--    comment out this section and use the Warehouse Runtime fallback in Phase 2.
-- -----------------------------------------------------------------------------
CREATE COMPUTE POOL IF NOT EXISTS ECOMM_APP_POOL
    MIN_NODES = 1
    MAX_NODES = 1
    INSTANCE_FAMILY = CPU_X64_XS
    AUTO_RESUME = TRUE
    AUTO_SUSPEND_SECS = 60
    COMMENT = 'X-Small compute pool for Streamlit Container Runtime';

GRANT USAGE, MONITOR ON COMPUTE POOL ECOMM_APP_POOL TO ROLE ECOMM_CICD_ROLE;
GRANT USAGE ON COMPUTE POOL ECOMM_APP_POOL TO ROLE ECOMM_APP_ROLE;

-- Needed by Container Runtime to fetch PyPI packages from external network
-- (Skip if your trial restricts External Access Integrations; Warehouse Runtime needs none)
CREATE OR REPLACE NETWORK RULE ECOMM_PYPI_NETWORK_RULE
    MODE = EGRESS
    TYPE = HOST_PORT
    VALUE_LIST = ('pypi.org', 'files.pythonhosted.org');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ECOMM_PYPI_ACCESS
    ALLOWED_NETWORK_RULES = (ECOMM_PYPI_NETWORK_RULE)
    ENABLED = TRUE;

GRANT USAGE ON INTEGRATION ECOMM_PYPI_ACCESS TO ROLE ECOMM_CICD_ROLE;

-- -----------------------------------------------------------------------------
-- 8. CI/CD Service User (MFA-exempt, Key-Pair Auth ONLY)
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS SVC_GITHUB_CICD
    TYPE = SERVICE
    DEFAULT_ROLE = ECOMM_CICD_ROLE
    DEFAULT_WAREHOUSE = ECOMM_ETL_WH
    DEFAULT_NAMESPACE = ECOMM_DEV.MART
    COMMENT = 'Non-interactive GitHub Actions deployment service user';

GRANT ROLE ECOMM_CICD_ROLE TO USER SVC_GITHUB_CICD;

-- -----------------------------------------------------------------------------
-- 9. Grant Dev & App Roles to YOUR Interactive Human User
--    Replace <YOUR_SNOWFLAKE_USERNAME> with your Snowsight login name and uncomment!
-- -----------------------------------------------------------------------------
-- GRANT ROLE ECOMM_CICD_ROLE TO USER <YOUR_SNOWFLAKE_USERNAME>;
-- GRANT ROLE ECOMM_APP_ROLE TO USER <YOUR_SNOWFLAKE_USERNAME>;
-- GRANT ROLE ECOMM_VIEWER_ROLE TO USER <YOUR_SNOWFLAKE_USERNAME>;
```

---

### Step 1.2 `[CREATE FILE]` `sql/02_schemas_and_stages.sql`

This script uses Snowflake CLI Jinja templating (`<% db %>`) so the **exact same script** runs against `ECOMM_DEV` and `ECOMM_PROD`.

```sql
-- =============================================================================
-- 02_schemas_and_stages.sql
-- Purpose : Idempotent schemas, file formats, and internal stage per environment
-- Run Via : snow sql -c dev -f sql/02_schemas_and_stages.sql -D "db=ECOMM_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE ECOMM_CICD_ROLE;
USE WAREHOUSE ECOMM_ETL_WH;
USE DATABASE <% db %>;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS MART;
CREATE SCHEMA IF NOT EXISTS APP;

CREATE FILE FORMAT IF NOT EXISTS RAW.PARQUET_FF
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_FF
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null');

CREATE STAGE IF NOT EXISTS RAW.ECOMM_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal named stage for raw e-commerce Parquet/CSV ingestion';
```

---

### Step 1.3 `[CREATE FILE]` `sql/03_raw_tables.sql`

```sql
-- =============================================================================
-- 03_raw_tables.sql
-- Purpose : Idempotent DDL for RAW landing tables
-- Run Via : snow sql -c dev -f sql/03_raw_tables.sql -D "db=ECOMM_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE ECOMM_CICD_ROLE;
USE WAREHOUSE ECOMM_ETL_WH;
USE DATABASE <% db %>;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW_CUSTOMERS (
    CUSTOMER_ID         VARCHAR(32) NOT NULL,
    SIGNUP_DATE         DATE        NOT NULL,
    REGION              VARCHAR(32) NOT NULL,
    ACQUISITION_CHANNEL VARCHAR(32) NOT NULL
);

CREATE TABLE IF NOT EXISTS RAW_PRODUCTS (
    SKU_ID        VARCHAR(32)   NOT NULL,
    PRODUCT_NAME  VARCHAR(128)  NOT NULL,
    CATEGORY      VARCHAR(64)   NOT NULL,
    UNIT_COST     NUMBER(10, 2) NOT NULL,
    UNIT_PRICE    NUMBER(10, 2) NOT NULL,
    STOCK_ON_HAND INTEGER       NOT NULL,
    REORDER_POINT INTEGER       NOT NULL
);

CREATE TABLE IF NOT EXISTS RAW_ORDERS (
    ORDER_ID        VARCHAR(32)   NOT NULL,
    CUSTOMER_ID     VARCHAR(32)   NOT NULL,
    ORDER_TS        TIMESTAMP_NTZ NOT NULL,
    ORDER_STATUS    VARCHAR(24)   NOT NULL,
    SHIPPING_COST   NUMBER(10, 2) NOT NULL,
    DISCOUNT_AMOUNT NUMBER(10, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS RAW_ORDER_ITEMS (
    ORDER_ITEM_ID VARCHAR(36)   NOT NULL,
    ORDER_ID      VARCHAR(32)   NOT NULL,
    SKU_ID        VARCHAR(32)   NOT NULL,
    QUANTITY      INTEGER       NOT NULL,
    UNIT_PRICE    NUMBER(10, 2) NOT NULL
);
```

---

### Step 1.4 `[CREATE FILE]` `scripts/generate_data.py`

This script uses **vectorized NumPy + Pandas + chunked PyArrow Parquet writes** so generating 50,000 rows takes ~1 second and generating **5,000,000 rows** (Phase 4) finishes in seconds without blowing up RAM.

```python
"""Synthetic e-commerce dataset generator (scalable from 50k to 5M+ line items)."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

REGIONS = ["North America", "Europe", "APAC", "LATAM"]
REGION_WEIGHTS = [0.45, 0.28, 0.18, 0.09]

CHANNELS = ["Organic", "Paid Search", "Social", "Email", "Referral"]
CHANNEL_WEIGHTS = [0.32, 0.26, 0.20, 0.14, 0.08]

CATEGORIES = [
    "Electronics",
    "Apparel",
    "Home & Kitchen",
    "Beauty",
    "Sports",
    "Accessories",
]

ADJECTIVES = [
    "Pro",
    "Ultra",
    "Compact",
    "Essential",
    "Classic",
    "Smart",
    "Eco",
    "Studio",
    "Travel",
    "Peak",
]

NOUNS_BY_CATEGORY = {
    "Electronics": ["Headphones", "Webcam", "Charger", "Speaker", "SSD", "Keyboard"],
    "Apparel": ["Hoodie", "Sneakers", "Jacket", "Tee", "Chinos", "Cap"],
    "Home & Kitchen": ["Blender", "Kettle", "Lamp", "Pan Set", "Air Purifier", "Mug"],
    "Beauty": ["Serum", "Cleanser", "Moisturizer", "Sunscreen", "Toner", "Mask"],
    "Sports": ["Yoga Mat", "Dumbbell", "Water Bottle", "Resistance Band", "Backpack"],
    "Accessories": ["Watch Band", "Phone Case", "Laptop Sleeve", "Sunglasses", "Wallet"],
}

ORDER_STATUSES = ["COMPLETED", "RETURNED", "CANCELLED"]
ORDER_STATUS_WEIGHTS = [0.88, 0.07, 0.05]


def generate_customers(
    num_customers: int,
    rng: np.random.Generator,
    start_date: str = "2024-01-01",
    end_date: str = "2025-12-31",
) -> pd.DataFrame:
    """Generate customer dimension rows with signup dates, regions, and channels."""
    start_ts = pd.Timestamp(start_date)
    total_days = (pd.Timestamp(end_date) - start_ts).days
    signup_offsets = rng.integers(0, max(total_days, 1), size=num_customers)
    signup_dates = (start_ts + pd.to_timedelta(signup_offsets, unit="D")).date

    return pd.DataFrame(
        {
            "customer_id": [f"CUST_{i:07d}" for i in range(1, num_customers + 1)],
            "signup_date": signup_dates,
            "region": rng.choice(REGIONS, size=num_customers, p=REGION_WEIGHTS),
            "acquisition_channel": rng.choice(CHANNELS, size=num_customers, p=CHANNEL_WEIGHTS),
        }
    )


def generate_products(num_products: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate SKU catalog where ~20% of SKUs intentionally sit at or below reorder point."""
    categories = rng.choice(CATEGORIES, size=num_products)
    names = [
        f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS_BY_CATEGORY[cat])} {i:03d}"
        for i, cat in enumerate(categories, start=1)
    ]

    unit_prices = np.round(rng.uniform(12.0, 420.0, size=num_products), 2)
    margin_ratios = rng.uniform(0.32, 0.72, size=num_products)
    unit_costs = np.round(unit_prices * (1.0 - margin_ratios), 2)

    reorder_points = rng.integers(15, 80, size=num_products)
    stock_on_hand = rng.integers(25, 400, size=num_products)

    # Flag ~20% as low-stock / stockout risk so Inventory Alerts view is immediately informative
    low_stock_mask = rng.random(size=num_products) < 0.20
    stock_on_hand[low_stock_mask] = rng.integers(0, reorder_points[low_stock_mask] + 1)

    return pd.DataFrame(
        {
            "sku_id": [f"SKU_{i:05d}" for i in range(1, num_products + 1)],
            "product_name": names,
            "category": categories,
            "unit_cost": unit_costs,
            "unit_price": unit_prices,
            "stock_on_hand": stock_on_hand.astype(np.int64),
            "reorder_point": reorder_points.astype(np.int64),
        }
    )


def generate_orders_chunk(
    chunk_idx: int,
    item_start_id: int,
    order_start_id: int,
    num_items: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    rng: np.random.Generator,
    max_order_date: date,
    unsorted: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Generate one chunk of referentially consistent orders and order line items."""
    # Average ~2 items per order
    num_orders = max(1, int(np.ceil(num_items / 2.0)))

    cust_indices = rng.integers(0, len(customers_df), size=num_orders)
    chosen_cust_ids = customers_df["customer_id"].to_numpy()[cust_indices]
    cust_signups = pd.to_datetime(customers_df["signup_date"].to_numpy()[cust_indices])

    max_ts = pd.Timestamp(max_order_date)
    days_since_signup = np.maximum(1, (max_ts - cust_signups).days.to_numpy())
    # Bias offsets slightly toward early repeat purchases for realistic cohort retention decay
    order_day_offsets = np.floor(rng.beta(0.85, 1.4, size=num_orders) * days_since_signup).astype(
        int
    )
    seconds_in_day = rng.integers(0, 86400, size=num_orders)
    order_timestamps = (
        cust_signups
        + pd.to_timedelta(order_day_offsets, unit="D")
        + pd.to_timedelta(seconds_in_day, unit="s")
    )

    order_ids = np.array([f"ORD_{order_start_id + i:09d}" for i in range(num_orders)])
    order_statuses = rng.choice(ORDER_STATUSES, size=num_orders, p=ORDER_STATUS_WEIGHTS)
    shipping_costs = np.round(rng.choice([0.0, 4.99, 7.99, 12.99], size=num_orders, p=[0.4, 0.3, 0.2, 0.1]), 2)
    discount_amounts = np.round(
        np.where(rng.random(num_orders) < 0.25, rng.uniform(5.0, 35.0, size=num_orders), 0.0), 2
    )

    orders_df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": chosen_cust_ids,
            "order_ts": order_timestamps.astype("datetime64[us]"),
            "order_status": order_statuses,
            "shipping_cost": shipping_costs,
            "discount_amount": discount_amounts,
        }
    )

    if unsorted:
        orders_df = orders_df.sample(frac=1.0, random_state=int(rng.integers(0, 1_000_000))).reset_index(
            drop=True
        )
    else:
        orders_df = orders_df.sort_values("order_ts").reset_index(drop=True)

    # Ensure every order gets at least 1 item, plus random extra items up to num_items
    if num_items >= num_orders:
        base_order_idx = np.arange(num_orders)
        extra_order_idx = rng.integers(0, num_orders, size=num_items - num_orders)
        item_order_idx = np.concatenate([base_order_idx, extra_order_idx])
    else:
        item_order_idx = rng.integers(0, num_orders, size=num_items)

    rng.shuffle(item_order_idx)
    sku_indices = rng.integers(0, len(products_df), size=num_items)

    order_items_df = pd.DataFrame(
        {
            "order_item_id": [f"ITEM_{item_start_id + i:010d}" for i in range(num_items)],
            "order_id": order_ids[item_order_idx],
            "sku_id": products_df["sku_id"].to_numpy()[sku_indices],
            "quantity": rng.integers(1, 5, size=num_items).astype(np.int64),
            "unit_price": products_df["unit_price"].to_numpy()[sku_indices],
        }
    )

    return orders_df, order_items_df, num_orders


def _write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False, compression="snappy")
    elif fmt == "csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def generate_dataset(
    rows: int,
    output_dir: Path,
    fmt: str = "parquet",
    seed: int = 42,
    unsorted: bool = False,
    chunk_size: int = 500_000,
) -> dict[str, int]:
    """Generate full synthetic e-commerce dataset into output_dir/{customers,products,orders,order_items}."""
    if rows <= 0:
        raise ValueError("--rows must be a positive integer.")

    rng = np.random.default_rng(seed)
    num_customers = min(max(1_000, rows // 10), 500_000)
    num_products = 250

    customers_df = generate_customers(num_customers=num_customers, rng=rng)
    products_df = generate_products(num_products=num_products, rng=rng)

    ext = "parquet" if fmt == "parquet" else "csv"
    _write_df(customers_df, output_dir / "customers" / f"customers.{ext}", fmt)
    _write_df(products_df, output_dir / "products" / f"products.{ext}", fmt)

    max_order_date = date(2026, 6, 30)
    remaining = rows
    chunk_idx = 0
    item_start = 1
    order_start = 1
    total_orders = 0

    while remaining > 0:
        cur_items = min(chunk_size, remaining)
        orders_df, items_df, chunk_orders = generate_orders_chunk(
            chunk_idx=chunk_idx,
            item_start_id=item_start,
            order_start_id=order_start,
            num_items=cur_items,
            customers_df=customers_df,
            products_df=products_df,
            rng=rng,
            max_order_date=max_order_date,
            unsorted=unsorted,
        )
        _write_df(orders_df, output_dir / "orders" / f"orders_part_{chunk_idx:04d}.{ext}", fmt)
        _write_df(
            items_df,
            output_dir / "order_items" / f"order_items_part_{chunk_idx:04d}.{ext}",
            fmt,
        )

        remaining -= cur_items
        item_start += cur_items
        order_start += chunk_orders
        total_orders += chunk_orders
        chunk_idx += 1

    return {
        "customers": num_customers,
        "products": num_products,
        "orders": total_orders,
        "order_items": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data for Snowflake.")
    parser.add_argument("--rows", type=int, default=50_000, help="Number of order line items.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Output directory.")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--unsorted",
        action="store_true",
        help="Shuffle order timestamps across chunks (used in Phase 4 clustering benchmark).",
    )
    parser.add_argument("--chunk-size", type=int, default=500_000)
    args = parser.parse_args()

    started = datetime.now()
    # Clean previous files inside output_dir subfolders if re-generating
    for sub in ("customers", "products", "orders", "order_items"):
        sub_dir = args.output_dir / sub
        if sub_dir.exists():
            for f in sub_dir.iterdir():
                if f.is_file():
                    f.unlink()

    counts = generate_dataset(
        rows=args.rows,
        output_dir=args.output_dir,
        fmt=args.format,
        seed=args.seed,
        unsorted=args.unsorted,
        chunk_size=args.chunk_size,
    )
    elapsed = (datetime.now() - started).total_seconds()
    print(f"Generated dataset in {elapsed:.2f}s -> {counts} at {args.output_dir}")


if __name__ == "__main__":
    main()
```

---

### Step 1.5 `[CREATE FILE]` `sql/04_load_copy_into.sql`

```sql
-- =============================================================================
-- 04_load_copy_into.sql
-- Purpose : Bulk-load Parquet data from @RAW.ECOMM_STAGE into RAW tables
-- Run Via : snow sql -c dev -f sql/04_load_copy_into.sql -D "db=ECOMM_DEV" --enable-templating JINJA
-- =============================================================================

USE ROLE ECOMM_CICD_ROLE;
USE WAREHOUSE ECOMM_ETL_WH;
USE DATABASE <% db %>;
USE SCHEMA RAW;

-- Full refresh of landing tables (idempotent for dev & benchmark reloads)
TRUNCATE TABLE IF EXISTS RAW_CUSTOMERS;
TRUNCATE TABLE IF EXISTS RAW_PRODUCTS;
TRUNCATE TABLE IF EXISTS RAW_ORDERS;
TRUNCATE TABLE IF EXISTS RAW_ORDER_ITEMS;

COPY INTO RAW_CUSTOMERS
FROM @RAW.ECOMM_STAGE/customers/
FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO RAW_PRODUCTS
FROM @RAW.ECOMM_STAGE/products/
FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO RAW_ORDERS
FROM @RAW.ECOMM_STAGE/orders/
FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

COPY INTO RAW_ORDER_ITEMS
FROM @RAW.ECOMM_STAGE/order_items/
FILE_FORMAT = (FORMAT_NAME = 'RAW.PARQUET_FF')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
ON_ERROR = 'ABORT_STATEMENT';

-- Verification counts
SELECT 'RAW_CUSTOMERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM RAW_CUSTOMERS
UNION ALL
SELECT 'RAW_PRODUCTS', COUNT(*) FROM RAW_PRODUCTS
UNION ALL
SELECT 'RAW_ORDERS', COUNT(*) FROM RAW_ORDERS
UNION ALL
SELECT 'RAW_ORDER_ITEMS', COUNT(*) FROM RAW_ORDER_ITEMS;
```

---

### Step 1.6 `[CREATE FILE]` `scripts/run_snowpark_etl.py`

This pipeline reads `RAW.*` via the **Snowpark Session API** and builds 5 clean `MART` tables (`FACT_ORDERS`, `DIM_CUSTOMERS`, `DIM_PRODUCTS`, `AGG_DAILY_REVENUE`, `AGG_COHORT_RETENTION`). Notice how order-level `DISCOUNT_AMOUNT` and `SHIPPING_COST` are proportionally allocated across line items so summing `FACT_ORDERS` never double-counts.

```python
"""Snowpark Session API ETL pipeline: transforms RAW landing tables into MART analytics tables."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.snowpark import Session, Window
from snowflake.snowpark import functions as F


def _load_private_key_bytes(key_path: str | None, key_pem: str | None, passphrase: str | None) -> bytes:
    """Decode an encrypted/unencrypted PKCS#8 PEM private key into DER bytes for Snowflake."""
    raw_pem = key_pem.encode("utf-8") if key_pem else Path(key_path or "").read_bytes()
    pwd = passphrase.encode("utf-8") if passphrase else None
    private_key = serialization.load_pem_private_key(raw_pem, password=pwd)
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def create_snowpark_session(database: str, connection_name: str | None = None) -> Session:
    """Create a Snowpark Session from a Snowflake CLI connection or env vars."""
    load_dotenv()

    if connection_name:
        session = Session.builder.config("connection_name", connection_name).create()
        session.sql(f"USE DATABASE {database}").collect()
        session.sql("USE WAREHOUSE ECOMM_ETL_WH").collect()
        return session

    cfg: dict[str, object] = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "role": os.getenv("SNOWFLAKE_ROLE", "ECOMM_CICD_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "ECOMM_ETL_WH"),
        "database": database,
        "schema": "MART",
    }

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    key_pem = os.getenv("SNOWFLAKE_PRIVATE_KEY")
    if key_path or key_pem:
        cfg["private_key"] = _load_private_key_bytes(
            key_path=key_path,
            key_pem=key_pem,
            passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE"),
        )
    elif os.getenv("SNOWFLAKE_PASSWORD"):
        cfg["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise RuntimeError(
            "Provide --connection <name> or SNOWFLAKE_PRIVATE_KEY(_PATH) env vars."
        )

    return Session.builder.configs(cfg).create()


def build_mart_tables(session: Session, database: str, sort_fact_orders: bool = False) -> None:
    """Transform RAW.* tables into MART.* analytical tables."""
    raw_customers = session.table(f"{database}.RAW.RAW_CUSTOMERS")
    raw_products = session.table(f"{database}.RAW.RAW_PRODUCTS")
    raw_orders = session.table(f"{database}.RAW.RAW_ORDERS")
    raw_items = session.table(f"{database}.RAW.RAW_ORDER_ITEMS")

    # 1. MART.FACT_ORDERS (Line-item grain with proportional order-level discount/shipping)
    order_window = Window.partition_by("ORDER_ID")
    line_gross = F.col("QUANTITY") * F.col("ITEM_UNIT_PRICE")

    joined = (
        raw_items.select(
            F.col("ORDER_ITEM_ID"),
            F.col("ORDER_ID"),
            F.col("SKU_ID"),
            F.col("QUANTITY"),
            F.col("UNIT_PRICE").alias("ITEM_UNIT_PRICE"),
        )
        .join(raw_orders, on="ORDER_ID", how="inner")
        .join(
            raw_products.select("SKU_ID", "PRODUCT_NAME", "CATEGORY", "UNIT_COST"),
            on="SKU_ID",
            how="inner",
        )
        .join(
            raw_customers.select("CUSTOMER_ID", "SIGNUP_DATE", "REGION", "ACQUISITION_CHANNEL"),
            on="CUSTOMER_ID",
            how="inner",
        )
        .with_column("ORDER_DATE", F.to_date(F.col("ORDER_TS")))
        .with_column("GROSS_REVENUE", line_gross)
        .with_column("ORDER_GROSS_TOTAL", F.sum("GROSS_REVENUE").over(order_window))
        .with_column("ORDER_ITEM_COUNT", F.count("ORDER_ITEM_ID").over(order_window))
    )

    alloc_ratio = F.iff(
        F.col("ORDER_GROSS_TOTAL") > 0,
        F.col("GROSS_REVENUE") / F.col("ORDER_GROSS_TOTAL"),
        F.lit(1.0) / F.col("ORDER_ITEM_COUNT"),
    )

    fact_orders = (
        joined.with_column(
            "ALLOCATED_DISCOUNT", F.round(F.col("DISCOUNT_AMOUNT") * alloc_ratio, 2)
        )
        .with_column("ALLOCATED_SHIPPING", F.round(F.col("SHIPPING_COST") * alloc_ratio, 2))
        .with_column(
            "NET_REVENUE", F.round(F.col("GROSS_REVENUE") - F.col("ALLOCATED_DISCOUNT"), 2)
        )
        .with_column("COGS", F.round(F.col("QUANTITY") * F.col("UNIT_COST"), 2))
        .with_column("NET_MARGIN", F.round(F.col("NET_REVENUE") - F.col("COGS"), 2))
        .select(
            "ORDER_ITEM_ID",
            "ORDER_ID",
            "CUSTOMER_ID",
            "SKU_ID",
            "PRODUCT_NAME",
            "CATEGORY",
            "REGION",
            "ACQUISITION_CHANNEL",
            "SIGNUP_DATE",
            "ORDER_TS",
            "ORDER_DATE",
            "ORDER_STATUS",
            "QUANTITY",
            "ITEM_UNIT_PRICE",
            "UNIT_COST",
            "GROSS_REVENUE",
            "ALLOCATED_DISCOUNT",
            "ALLOCATED_SHIPPING",
            "NET_REVENUE",
            "COGS",
            "NET_MARGIN",
        )
    )

    if sort_fact_orders:
        fact_orders = fact_orders.sort(F.col("ORDER_DATE").asc(), F.col("CATEGORY").asc())

    fact_orders.write.mode("overwrite").save_as_table(f"{database}.MART.FACT_ORDERS")
    fact_df = session.table(f"{database}.MART.FACT_ORDERS")
    completed = fact_df.filter(F.col("ORDER_STATUS") == F.lit("COMPLETED"))

    # 2. MART.DIM_CUSTOMERS
    cust_agg = completed.group_by("CUSTOMER_ID").agg(
        F.min("ORDER_DATE").alias("FIRST_ORDER_DATE"),
        F.max("ORDER_DATE").alias("LAST_ORDER_DATE"),
        F.count_distinct("ORDER_ID").alias("ORDER_COUNT"),
        F.sum("NET_REVENUE").alias("LIFETIME_REVENUE"),
        F.sum("NET_MARGIN").alias("LIFETIME_MARGIN"),
    )

    dim_customers = (
        raw_customers.join(cust_agg, on="CUSTOMER_ID", how="left")
        .with_column("SIGNUP_MONTH", F.date_trunc("MONTH", F.col("SIGNUP_DATE")))
        .with_column("ORDER_COUNT", F.coalesce(F.col("ORDER_COUNT"), F.lit(0)))
        .with_column("LIFETIME_REVENUE", F.coalesce(F.col("LIFETIME_REVENUE"), F.lit(0.0)))
        .with_column("LIFETIME_MARGIN", F.coalesce(F.col("LIFETIME_MARGIN"), F.lit(0.0)))
        .with_column(
            "AVG_ORDER_VALUE",
            F.iff(
                F.col("ORDER_COUNT") > 0,
                F.round(F.col("LIFETIME_REVENUE") / F.col("ORDER_COUNT"), 2),
                F.lit(0.0),
            ),
        )
        .with_column(
            "CUSTOMER_SEGMENT",
            F.when(F.col("ORDER_COUNT") == 0, F.lit("Prospect"))
            .when(F.col("ORDER_COUNT") == 1, F.lit("One-Time"))
            .when(F.col("LIFETIME_REVENUE") >= 500, F.lit("VIP"))
            .otherwise(F.lit("Repeat")),
        )
    )
    dim_customers.write.mode("overwrite").save_as_table(f"{database}.MART.DIM_CUSTOMERS")

    # 3. MART.DIM_PRODUCTS
    max_date_row = completed.agg(F.max("ORDER_DATE").alias("MAX_D")).collect()[0]
    max_order_date = max_date_row["MAX_D"]
    cutoff_30d = F.dateadd("DAY", F.lit(-30), F.lit(max_order_date))

    sku_lifetime = completed.group_by("SKU_ID").agg(
        F.sum("QUANTITY").alias("TOTAL_UNITS_SOLD"),
        F.sum("NET_REVENUE").alias("TOTAL_REVENUE"),
        F.sum("NET_MARGIN").alias("TOTAL_NET_MARGIN"),
    )
    sku_30d = (
        completed.filter(F.col("ORDER_DATE") >= cutoff_30d)
        .group_by("SKU_ID")
        .agg(F.sum("QUANTITY").alias("UNITS_SOLD_30D"))
    )

    dim_products = (
        raw_products.join(sku_lifetime, on="SKU_ID", how="left")
        .join(sku_30d, on="SKU_ID", how="left")
        .with_column("TOTAL_UNITS_SOLD", F.coalesce(F.col("TOTAL_UNITS_SOLD"), F.lit(0)))
        .with_column("TOTAL_REVENUE", F.coalesce(F.col("TOTAL_REVENUE"), F.lit(0.0)))
        .with_column("TOTAL_NET_MARGIN", F.coalesce(F.col("TOTAL_NET_MARGIN"), F.lit(0.0)))
        .with_column("UNITS_SOLD_30D", F.coalesce(F.col("UNITS_SOLD_30D"), F.lit(0)))
        .with_column("DAILY_VELOCITY_30D", F.round(F.col("UNITS_SOLD_30D") / F.lit(30.0), 2))
        .with_column(
            "DAYS_OF_SUPPLY",
            F.iff(
                F.col("DAILY_VELOCITY_30D") > 0,
                F.round(F.col("STOCK_ON_HAND") / F.col("DAILY_VELOCITY_30D"), 1),
                F.lit(999.0),
            ),
        )
        .with_column(
            "UNIT_MARGIN_PCT",
            F.round((F.col("UNIT_PRICE") - F.col("UNIT_COST")) / F.col("UNIT_PRICE") * 100.0, 2),
        )
        .with_column("STOCKOUT_RISK_FLAG", F.col("STOCK_ON_HAND") <= F.col("REORDER_POINT"))
    )
    dim_products.write.mode("overwrite").save_as_table(f"{database}.MART.DIM_PRODUCTS")

    # 4. MART.AGG_DAILY_REVENUE (Pre-aggregated fast layer for Executive KPI view)
    agg_daily = (
        completed.group_by("ORDER_DATE", "CATEGORY", "REGION")
        .agg(
            F.count_distinct("ORDER_ID").alias("ORDER_COUNT"),
            F.count_distinct("CUSTOMER_ID").alias("UNIQUE_CUSTOMERS"),
            F.sum("QUANTITY").alias("UNITS_SOLD"),
            F.sum("GROSS_REVENUE").alias("GROSS_REVENUE"),
            F.sum("NET_REVENUE").alias("NET_REVENUE"),
            F.sum("COGS").alias("TOTAL_COGS"),
            F.sum("NET_MARGIN").alias("NET_MARGIN"),
        )
        .with_column(
            "AOV",
            F.iff(
                F.col("ORDER_COUNT") > 0,
                F.round(F.col("NET_REVENUE") / F.col("ORDER_COUNT"), 2),
                F.lit(0.0),
            ),
        )
    )
    agg_daily.write.mode("overwrite").save_as_table(f"{database}.MART.AGG_DAILY_REVENUE")

    # 5. MART.AGG_COHORT_RETENTION (Pre-aggregated monthly signup retention matrix)
    cohort_sizes = (
        raw_customers.with_column("COHORT_MONTH", F.date_trunc("MONTH", F.col("SIGNUP_DATE")))
        .group_by("COHORT_MONTH")
        .agg(F.count_distinct("CUSTOMER_ID").alias("COHORT_SIZE"))
    )

    active_by_month = (
        completed.with_column("COHORT_MONTH", F.date_trunc("MONTH", F.col("SIGNUP_DATE")))
        .with_column("ORDER_MONTH", F.date_trunc("MONTH", F.col("ORDER_DATE")))
        .with_column(
            "MONTHS_SINCE_SIGNUP",
            F.months_between(F.col("ORDER_MONTH"), F.col("COHORT_MONTH")).cast("INTEGER"),
        )
        .filter(F.col("MONTHS_SINCE_SIGNUP") >= 0)
        .group_by("COHORT_MONTH", "MONTHS_SINCE_SIGNUP")
        .agg(F.count_distinct("CUSTOMER_ID").alias("ACTIVE_CUSTOMERS"))
    )

    agg_cohort = (
        active_by_month.join(cohort_sizes, on="COHORT_MONTH", how="inner")
        .with_column(
            "RETENTION_RATE",
            F.round((F.col("ACTIVE_CUSTOMERS") / F.col("COHORT_SIZE")) * 100.0, 2),
        )
        .sort(F.col("COHORT_MONTH").asc(), F.col("MONTHS_SINCE_SIGNUP").asc())
    )
    agg_cohort.write.mode("overwrite").save_as_table(f"{database}.MART.AGG_COHORT_RETENTION")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Snowpark RAW -> MART ETL.")
    parser.add_argument("--database", default="ECOMM_DEV", choices=["ECOMM_DEV", "ECOMM_PROD"])
    parser.add_argument("--connection", default=None, help="Snowflake CLI connection name (e.g. dev)")
    parser.add_argument(
        "--sort-fact-orders",
        action="store_true",
        help="Sort FACT_ORDERS by (ORDER_DATE, CATEGORY) on write.",
    )
    args = parser.parse_args()

    session = create_snowpark_session(database=args.database, connection_name=args.connection)
    try:
        build_mart_tables(
            session=session,
            database=args.database,
            sort_fact_orders=args.sort_fact_orders,
        )
        print(f"Successfully built all 5 MART tables in {args.database}.MART")
    finally:
        session.close()


if __name__ == "__main__":
    main()
```

---

### Step 1.7 `[SNOWSIGHT UI + LOCAL TERMINAL]` Execute Bootstrap, Stage, Load & ETL

1. **`[SNOWSIGHT UI]`**:
   - Open `sql/01_bootstrap_account.sql` in Snowsight as **`ACCOUNTADMIN`**.
   - Find your Snowflake username by running `SELECT CURRENT_USER();`.
   - Uncomment the last 3 lines of `01_bootstrap_account.sql`, replace `<YOUR_SNOWFLAKE_USERNAME>` with your username, and **Run All**.
   - *(If your trial region rejects `CREATE COMPUTE POOL`, simply comment out Section 7 of `01_bootstrap_account.sql` and proceed—Phase 2 includes the Warehouse Runtime fallback).*

2. **`[LOCAL TERMINAL]`** Configure a local Snowflake CLI connection named `dev`:

```bash
source .venv/bin/activate

snow connection add \
  --connection-name dev \
  --account "<YOUR_ACCOUNT_IDENTIFIER>" \
  --user "<YOUR_SNOWFLAKE_USERNAME>" \
  --role "ECOMM_CICD_ROLE" \
  --warehouse "ECOMM_ETL_WH" \
  --database "ECOMM_DEV" \
  --schema "MART" \
  --default
```

*(Note: When prompted for password/authenticator, enter your Snowflake login credentials. If your trial enforces browser MFA on human logins, pass `--authenticator externalbrowser` when adding the connection.)*

3. **`[LOCAL TERMINAL]`** Generate dev data (50,000 line items), create schemas/tables, upload to stage, run `COPY INTO`, and run Snowpark ETL:

```bash
# 1. Generate 50k synthetic Parquet rows
python scripts/generate_data.py --rows 50000 --output-dir data/raw --format parquet

# 2. Create schemas, file formats, internal stage, and RAW tables in ECOMM_DEV
snow sql -c dev -f sql/02_schemas_and_stages.sql -D "db=ECOMM_DEV" --enable-templating JINJA
snow sql -c dev -f sql/03_raw_tables.sql -D "db=ECOMM_DEV" --enable-templating JINJA

# 3. Upload Parquet folders to internal stage
snow stage copy data/raw/customers "@ECOMM_DEV.RAW.ECOMM_STAGE/customers" --overwrite -c dev
snow stage copy data/raw/products "@ECOMM_DEV.RAW.ECOMM_STAGE/products" --overwrite -c dev
snow stage copy data/raw/orders "@ECOMM_DEV.RAW.ECOMM_STAGE/orders" --overwrite -c dev
snow stage copy data/raw/order_items "@ECOMM_DEV.RAW.ECOMM_STAGE/order_items" --overwrite -c dev

# 4. Bulk load via COPY INTO
snow sql -c dev -f sql/04_load_copy_into.sql -D "db=ECOMM_DEV" --enable-templating JINJA

# 5. Transform RAW -> MART via Snowpark Session API
python scripts/run_snowpark_etl.py --database ECOMM_DEV --connection dev
```

---

### Step 1.8 `[VERIFY]` Phase 1 Definition of Done

Run this inline RBAC + row-count verification via `snow sql`:

```bash
# Check all 5 MART tables have rows
snow sql -c dev -q "
SELECT 'FACT_ORDERS' AS TBL, COUNT(*) AS CNT FROM ECOMM_DEV.MART.FACT_ORDERS
UNION ALL SELECT 'DIM_CUSTOMERS', COUNT(*) FROM ECOMM_DEV.MART.DIM_CUSTOMERS
UNION ALL SELECT 'DIM_PRODUCTS', COUNT(*) FROM ECOMM_DEV.MART.DIM_PRODUCTS
UNION ALL SELECT 'AGG_DAILY_REVENUE', COUNT(*) FROM ECOMM_DEV.MART.AGG_DAILY_REVENUE
UNION ALL SELECT 'AGG_COHORT_RETENTION', COUNT(*) FROM ECOMM_DEV.MART.AGG_COHORT_RETENTION;
"

# Verify ECOMM_APP_ROLE CAN read MART
snow sql -c dev -q "USE ROLE ECOMM_APP_ROLE; SELECT COUNT(*) FROM ECOMM_DEV.MART.AGG_DAILY_REVENUE;"

# Verify ECOMM_APP_ROLE is DENIED on RAW (must fail with SQL access control error!)
snow sql -c dev -q "USE ROLE ECOMM_APP_ROLE; SELECT COUNT(*) FROM ECOMM_DEV.RAW.RAW_ORDERS;"
```

**Expected result:**
* All 5 `MART` tables return counts > 0 (`FACT_ORDERS` ≈ 50,000).
* `ECOMM_APP_ROLE` succeeds on `MART.AGG_DAILY_REVENUE` and **fails with `does not exist or not authorized`** on `RAW.RAW_ORDERS`.

---
## Phase 2: Pillar 2 — Streamlit in Snowflake (SiS) Application & Tests

**Goal:** Build the portable, cached, strictly parameterized Streamlit analytics application (`app/`), configure `snowflake.yml` for Container Runtime (with a documented Warehouse Runtime fallback), write comprehensive unit tests (`tests/`), run locally, deploy to `ECOMM_DEV.APP`, and verify `@st.cache_data`.

---

### Step 2.1 `[CREATE FILE]` Package Init & Session Layer

#### 1. Create `app/utils/__init__.py` (empty file)

```bash
touch app/utils/__init__.py app/views/__init__.py tests/__init__.py
```

#### 2. Create `app/utils/snowflake_session.py`

```python
"""Unified Snowflake session resolver for Streamlit in Snowflake (SiS) and local dev."""

from __future__ import annotations

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def is_running_in_sis() -> bool:
    """Return True when executing inside Streamlit in Snowflake."""
    try:
        get_active_session()
        return True
    except Exception:
        return False


def get_snowflake_session() -> Session:
    """Return active Snowpark Session inside SiS, or fall back to local st.connection."""
    try:
        return get_active_session()
    except Exception:
        conn = st.connection("snowflake")
        return conn.session()
```

---

### Step 2.2 `[CREATE FILE]` `app/utils/queries.py`

Notice three engineering patterns here:
1. **Unqualified `MART.<TABLE>`** — works seamlessly in both `ECOMM_DEV` and `ECOMM_PROD`.
2. **`_session` prefix** — tells `@st.cache_data(ttl=600)` not to hash the Snowpark `Session` object, while still keying the cache on every filter argument.
3. **`?` bound parameters everywhere** — zero string interpolation of user-supplied values.

```python
"""Cached, strictly parameterized queries against the MART schema."""

from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st
from snowflake.snowpark import Session


def build_in_placeholders(count: int) -> str:
    """Return comma-separated '?' placeholders for SQL IN clauses."""
    if count <= 0:
        raise ValueError("Placeholder count must be at least 1.")
    return ", ".join(["?"] * count)


def compute_kpi_metrics(daily_df: pd.DataFrame) -> dict[str, float]:
    """Compute aggregate KPI metrics from AGG_DAILY_REVENUE rows.

    Always divides total NET_REVENUE by total ORDER_COUNT rather than averaging
    pre-computed daily AOVs (avoids Simpson's paradox).
    """
    if daily_df.empty:
        return {
            "total_revenue": 0.0,
            "total_orders": 0.0,
            "aov": 0.0,
            "net_margin": 0.0,
            "margin_pct": 0.0,
        }

    total_revenue = float(daily_df["NET_REVENUE"].sum())
    total_orders = float(daily_df["ORDER_COUNT"].sum())
    net_margin = float(daily_df["NET_MARGIN"].sum())
    aov = total_revenue / total_orders if total_orders > 0 else 0.0
    margin_pct = (net_margin / total_revenue * 100.0) if total_revenue > 0 else 0.0

    return {
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
        "aov": round(aov, 2),
        "net_margin": round(net_margin, 2),
        "margin_pct": round(margin_pct, 2),
    }


def compute_mom_growth(current: float, previous: float) -> float | None:
    """Compute percentage change between current and previous periods."""
    if previous <= 0:
        return None
    return round(((current - previous) / previous) * 100.0, 2)


@st.cache_data(ttl=600, show_spinner=False)
def get_filter_metadata(_session: Session) -> dict[str, object]:
    """Fetch min/max order dates, categories, and regions from MART.AGG_DAILY_REVENUE."""
    bounds_df = _session.sql(
        """
        SELECT
            MIN(ORDER_DATE) AS MIN_DATE,
            MAX(ORDER_DATE) AS MAX_DATE
        FROM MART.AGG_DAILY_REVENUE
        """
    ).to_pandas()

    cats_df = _session.sql(
        "SELECT DISTINCT CATEGORY FROM MART.AGG_DAILY_REVENUE ORDER BY CATEGORY"
    ).to_pandas()

    regions_df = _session.sql(
        "SELECT DISTINCT REGION FROM MART.AGG_DAILY_REVENUE ORDER BY REGION"
    ).to_pandas()

    min_date = pd.to_datetime(bounds_df.iloc[0]["MIN_DATE"]).date()
    max_date = pd.to_datetime(bounds_df.iloc[0]["MAX_DATE"]).date()

    return {
        "min_date": min_date,
        "max_date": max_date,
        "categories": cats_df["CATEGORY"].tolist(),
        "regions": regions_df["REGION"].tolist(),
    }


@st.cache_data(ttl=600, show_spinner=False)
def get_daily_revenue(
    _session: Session,
    start_date: date,
    end_date: date,
    categories: tuple[str, ...],
    regions: tuple[str, ...],
) -> pd.DataFrame:
    """Query pre-aggregated daily revenue filtered by date, categories, and regions."""
    if not categories or not regions:
        return pd.DataFrame(
            columns=[
                "ORDER_DATE",
                "CATEGORY",
                "REGION",
                "ORDER_COUNT",
                "UNIQUE_CUSTOMERS",
                "UNITS_SOLD",
                "GROSS_REVENUE",
                "NET_REVENUE",
                "TOTAL_COGS",
                "NET_MARGIN",
            ]
        )

    cat_ph = build_in_placeholders(len(categories))
    reg_ph = build_in_placeholders(len(regions))
    sql = f"""
        SELECT
            ORDER_DATE,
            CATEGORY,
            REGION,
            ORDER_COUNT,
            UNIQUE_CUSTOMERS,
            UNITS_SOLD,
            GROSS_REVENUE,
            NET_REVENUE,
            TOTAL_COGS,
            NET_MARGIN
        FROM MART.AGG_DAILY_REVENUE
        WHERE ORDER_DATE BETWEEN ? AND ?
          AND CATEGORY IN ({cat_ph})
          AND REGION IN ({reg_ph})
        ORDER BY ORDER_DATE ASC
    """
    params: list[object] = [start_date.isoformat(), end_date.isoformat(), *categories, *regions]
    df = _session.sql(sql, params=params).to_pandas()
    if not df.empty:
        df["ORDER_DATE"] = pd.to_datetime(df["ORDER_DATE"])
    return df


@st.cache_data(ttl=600, show_spinner=False)
def get_cohort_retention(_session: Session, max_months: int = 12) -> pd.DataFrame:
    """Fetch pre-aggregated monthly signup cohort retention matrix."""
    sql = """
        SELECT
            TO_VARCHAR(COHORT_MONTH, 'YYYY-MM') AS COHORT_MONTH,
            MONTHS_SINCE_SIGNUP,
            COHORT_SIZE,
            ACTIVE_CUSTOMERS,
            RETENTION_RATE
        FROM MART.AGG_COHORT_RETENTION
        WHERE MONTHS_SINCE_SIGNUP <= ?
        ORDER BY COHORT_MONTH ASC, MONTHS_SINCE_SIGNUP ASC
    """
    return _session.sql(sql, params=[int(max_months)]).to_pandas()


@st.cache_data(ttl=600, show_spinner=False)
def get_inventory_health(
    _session: Session,
    categories: tuple[str, ...],
    only_at_risk: bool = False,
) -> pd.DataFrame:
    """Query SKU inventory velocity, stockout risk, and unit margin from MART.DIM_PRODUCTS."""
    if not categories:
        return pd.DataFrame()

    cat_ph = build_in_placeholders(len(categories))
    risk_clause = "AND STOCKOUT_RISK_FLAG = TRUE" if only_at_risk else ""
    sql = f"""
        SELECT
            SKU_ID,
            PRODUCT_NAME,
            CATEGORY,
            UNIT_COST,
            UNIT_PRICE,
            UNIT_MARGIN_PCT,
            STOCK_ON_HAND,
            REORDER_POINT,
            UNITS_SOLD_30D,
            DAILY_VELOCITY_30D,
            DAYS_OF_SUPPLY,
            STOCKOUT_RISK_FLAG,
            TOTAL_REVENUE,
            TOTAL_NET_MARGIN
        FROM MART.DIM_PRODUCTS
        WHERE CATEGORY IN ({cat_ph})
          {risk_clause}
        ORDER BY STOCKOUT_RISK_FLAG DESC, DAYS_OF_SUPPLY ASC
    """
    return _session.sql(sql, params=[*categories]).to_pandas()


@st.cache_data(ttl=600, show_spinner=False)
def lookup_orders(
    _session: Session,
    search_type: str,
    search_value: str,
    limit: int = 200,
) -> pd.DataFrame:
    """Parameterized order or customer drill-down against MART.FACT_ORDERS."""
    cleaned = search_value.strip()
    if not cleaned:
        return pd.DataFrame()

    if search_type == "Order ID":
        filter_col = "ORDER_ID"
    elif search_type == "Customer ID":
        filter_col = "CUSTOMER_ID"
    else:
        raise ValueError(f"Unsupported search_type: {search_type}")

    sql = f"""
        SELECT
            ORDER_ID,
            ORDER_ITEM_ID,
            CUSTOMER_ID,
            ORDER_DATE,
            ORDER_STATUS,
            REGION,
            SKU_ID,
            PRODUCT_NAME,
            CATEGORY,
            QUANTITY,
            ITEM_UNIT_PRICE,
            GROSS_REVENUE,
            ALLOCATED_DISCOUNT,
            ALLOCATED_SHIPPING,
            NET_REVENUE,
            NET_MARGIN
        FROM MART.FACT_ORDERS
        WHERE {filter_col} = ?
        ORDER BY ORDER_DATE DESC, ORDER_ITEM_ID ASC
        LIMIT ?
    """
    return _session.sql(sql, params=[cleaned, int(limit)]).to_pandas()
```

---

### Step 2.3 `[CREATE FILE]` Modular Views in `app/views/`

#### 1. Create `app/views/executive_kpi.py`

```python
"""Executive KPIs view: revenue, orders, AOV, net margin, and trend charts."""

from __future__ import annotations

from datetime import timedelta

import altair as alt
import pandas as pd
import streamlit as st
from snowflake.snowpark import Session

from app.utils.queries import (
    compute_kpi_metrics,
    compute_mom_growth,
    get_daily_revenue,
    get_filter_metadata,
)


def render(session: Session) -> None:
    """Render the Executive KPIs page."""
    st.header("Executive KPI Overview")
    st.caption("Powered by pre-aggregated `MART.AGG_DAILY_REVENUE` with `@st.cache_data(ttl=600)`.")

    meta = get_filter_metadata(session)
    min_date = meta["min_date"]
    max_date = meta["max_date"]
    all_categories: list[str] = meta["categories"]  # type: ignore[assignment]
    all_regions: list[str] = meta["regions"]  # type: ignore[assignment]

    default_start = max(min_date, max_date - timedelta(days=90))

    col_f1, col_f2, col_f3 = st.columns([1.2, 1.4, 1.4])
    with col_f1:
        date_range = st.date_input(
            "Order Date Range",
            value=(default_start, max_date),
            min_value=min_date,
            max_value=max_date,
        )
    with col_f2:
        selected_cats = st.multiselect("Categories", options=all_categories, default=all_categories)
    with col_f3:
        selected_regions = st.multiselect("Regions", options=all_regions, default=all_regions)

    if not isinstance(date_range, tuple) or len(date_range) != 2:
        st.info("Select both a start and end date.")
        return

    start_date, end_date = date_range
    df = get_daily_revenue(
        session,
        start_date=start_date,
        end_date=end_date,
        categories=tuple(selected_cats),
        regions=tuple(selected_regions),
    )

    if df.empty:
        st.warning("No matching revenue records for the selected filters.")
        return

    kpis = compute_kpi_metrics(df)

    # Prior-period comparison window for MoM / period-over-period delta
    window_days = max(1, (end_date - start_date).days + 1)
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=window_days - 1)
    prev_df = get_daily_revenue(
        session,
        start_date=prev_start,
        end_date=prev_end,
        categories=tuple(selected_cats),
        regions=tuple(selected_regions),
    )
    prev_kpis = compute_kpi_metrics(prev_df)
    rev_delta = compute_mom_growth(kpis["total_revenue"], prev_kpis["total_revenue"])
    ord_delta = compute_mom_growth(kpis["total_orders"], prev_kpis["total_orders"])
    margin_delta = compute_mom_growth(kpis["net_margin"], prev_kpis["net_margin"])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric(
        "Net Revenue",
        f"${kpis['total_revenue']:,.2f}",
        delta=f"{rev_delta:+.1f}% vs prior" if rev_delta is not None else None,
    )
    m2.metric(
        "Completed Orders",
        f"{int(kpis['total_orders']):,}",
        delta=f"{ord_delta:+.1f}% vs prior" if ord_delta is not None else None,
    )
    m3.metric("Average Order Value", f"${kpis['aov']:,.2f}")
    m4.metric(
        "Net Margin",
        f"${kpis['net_margin']:,.2f} ({kpis['margin_pct']:.1f}%)",
        delta=f"{margin_delta:+.1f}% vs prior" if margin_delta is not None else None,
    )

    daily_trend = (
        df.groupby("ORDER_DATE", as_index=False)[["NET_REVENUE", "NET_MARGIN", "ORDER_COUNT"]]
        .sum()
        .sort_values("ORDER_DATE")
    )
    trend_chart = (
        alt.Chart(daily_trend)
        .mark_line(point=False, strokeWidth=2.5)
        .encode(
            x=alt.X("ORDER_DATE:T", title="Order Date"),
            y=alt.Y("NET_REVENUE:Q", title="Net Revenue ($)"),
            tooltip=[
                alt.Tooltip("ORDER_DATE:T", title="Date"),
                alt.Tooltip("NET_REVENUE:Q", title="Net Revenue", format="$,.2f"),
                alt.Tooltip("NET_MARGIN:Q", title="Net Margin", format="$,.2f"),
                alt.Tooltip("ORDER_COUNT:Q", title="Orders", format=","),
            ],
        )
        .properties(title="Daily Net Revenue Trend", height=320)
    )
    st.altair_chart(trend_chart, use_container_width=True)

    c_left, c_right = st.columns(2)
    with c_left:
        by_cat = df.groupby("CATEGORY", as_index=False)["NET_REVENUE"].sum()
        cat_chart = (
            alt.Chart(by_cat)
            .mark_bar()
            .encode(
                x=alt.X("NET_REVENUE:Q", title="Net Revenue ($)"),
                y=alt.Y("CATEGORY:N", sort="-x", title="Category"),
                tooltip=["CATEGORY", alt.Tooltip("NET_REVENUE:Q", format="$,.2f")],
            )
            .properties(title="Net Revenue by Category", height=280)
        )
        st.altair_chart(cat_chart, use_container_width=True)

    with c_right:
        by_reg = df.groupby("REGION", as_index=False)["NET_REVENUE"].sum()
        reg_chart = (
            alt.Chart(by_reg)
            .mark_bar()
            .encode(
                x=alt.X("NET_REVENUE:Q", title="Net Revenue ($)"),
                y=alt.Y("REGION:N", sort="-x", title="Region"),
                tooltip=["REGION", alt.Tooltip("NET_REVENUE:Q", format="$,.2f")],
            )
            .properties(title="Net Revenue by Region", height=280)
        )
        st.altair_chart(reg_chart, use_container_width=True)
```

#### 2. Create `app/views/cohort_retention.py`

```python
"""Cohort Retention view: monthly customer repeat purchase heatmap."""

from __future__ import annotations

import altair as alt
import streamlit as st
from snowflake.snowpark import Session

from app.utils.queries import get_cohort_retention


def render(session: Session) -> None:
    """Render the Monthly Customer Cohort Retention matrix."""
    st.header("Customer Cohort Retention")
    st.caption("Reads pre-aggregated monthly signup cohorts from `MART.AGG_COHORT_RETENTION`.")

    max_months = st.slider("Max Months Since Signup", min_value=3, max_value=24, value=12)
    df = get_cohort_retention(session, max_months=max_months)

    if df.empty:
        st.warning("No cohort retention data available.")
        return

    heatmap = (
        alt.Chart(df)
        .mark_rect()
        .encode(
            x=alt.X("MONTHS_SINCE_SIGNUP:O", title="Months Since Signup"),
            y=alt.Y("COHORT_MONTH:O", title="Signup Cohort"),
            color=alt.Color(
                "RETENTION_RATE:Q",
                title="Retention %",
                scale=alt.Scale(scheme="blues"),
            ),
            tooltip=[
                alt.Tooltip("COHORT_MONTH:O", title="Cohort"),
                alt.Tooltip("MONTHS_SINCE_SIGNUP:O", title="Month #"),
                alt.Tooltip("COHORT_SIZE:Q", title="Cohort Size", format=","),
                alt.Tooltip("ACTIVE_CUSTOMERS:Q", title="Active Customers", format=","),
                alt.Tooltip("RETENTION_RATE:Q", title="Retention %", format=".2f"),
            ],
        )
        .properties(title="Monthly Repeat Purchase Retention (%)", height=440)
    )
    st.altair_chart(heatmap, use_container_width=True)

    with st.expander("View Cohort Matrix Table"):
        pivot = df.pivot(
            index="COHORT_MONTH", columns="MONTHS_SINCE_SIGNUP", values="RETENTION_RATE"
        )
        st.dataframe(pivot, use_container_width=True)
```

#### 3. Create `app/views/inventory_alerts.py`

```python
"""Inventory & SKU Health view: stockout alerts, sell-through velocity, and margins."""

from __future__ import annotations

import altair as alt
import streamlit as st
from snowflake.snowpark import Session

from app.utils.queries import get_filter_metadata, get_inventory_health


def render(session: Session) -> None:
    """Render the Inventory & SKU Health page."""
    st.header("Inventory & SKU Margin Health")
    st.caption("Reads SKU sell-through velocity & stockout flags from `MART.DIM_PRODUCTS`.")

    meta = get_filter_metadata(session)
    all_categories: list[str] = meta["categories"]  # type: ignore[assignment]

    c1, c2 = st.columns([2, 1])
    with c1:
        selected_cats = st.multiselect("Filter Categories", options=all_categories, default=all_categories)
    with c2:
        only_at_risk = st.toggle("Show Stockout Risk Only", value=False)

    df = get_inventory_health(
        session,
        categories=tuple(selected_cats),
        only_at_risk=only_at_risk,
    )

    if df.empty:
        st.info("No SKUs match the selected inventory filters.")
        return

    at_risk_count = int(df["STOCKOUT_RISK_FLAG"].sum())
    avg_margin = float(df["UNIT_MARGIN_PCT"].mean())
    total_units = int(df["STOCK_ON_HAND"].sum())

    k1, k2, k3 = st.columns(3)
    k1.metric("SKUs Matching Filter", f"{len(df):,}")
    k2.metric("At-Risk SKUs (<= Reorder Pt)", f"{at_risk_count:,}")
    k3.metric("Average Unit Margin", f"{avg_margin:.1f}%", help=f"Total On-Hand Units: {total_units:,}")

    st.subheader("SKU Inventory & Velocity Table")
    st.dataframe(
        df[
            [
                "SKU_ID",
                "PRODUCT_NAME",
                "CATEGORY",
                "STOCK_ON_HAND",
                "REORDER_POINT",
                "DAILY_VELOCITY_30D",
                "DAYS_OF_SUPPLY",
                "UNIT_MARGIN_PCT",
                "STOCKOUT_RISK_FLAG",
                "TOTAL_NET_MARGIN",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )

    top_col, bot_col = st.columns(2)
    with top_col:
        top10 = df.nlargest(10, "TOTAL_NET_MARGIN")
        top_chart = (
            alt.Chart(top10)
            .mark_bar(color="#2e7d32")
            .encode(
                x=alt.X("TOTAL_NET_MARGIN:Q", title="Total Net Margin ($)"),
                y=alt.Y("PRODUCT_NAME:N", sort="-x", title="Product"),
                tooltip=["SKU_ID", "PRODUCT_NAME", alt.Tooltip("TOTAL_NET_MARGIN:Q", format="$,.2f")],
            )
            .properties(title="Top 10 Margin-Generating SKUs", height=300)
        )
        st.altair_chart(top_chart, use_container_width=True)

    with bot_col:
        bot10 = df.nsmallest(10, "UNIT_MARGIN_PCT")
        bot_chart = (
            alt.Chart(bot10)
            .mark_bar(color="#c62828")
            .encode(
                x=alt.X("UNIT_MARGIN_PCT:Q", title="Unit Margin (%)"),
                y=alt.Y("PRODUCT_NAME:N", sort="x", title="Product"),
                tooltip=["SKU_ID", "PRODUCT_NAME", alt.Tooltip("UNIT_MARGIN_PCT:Q", format=".2f")],
            )
            .properties(title="Lowest 10 Unit Margin % SKUs", height=300)
        )
        st.altair_chart(bot_chart, use_container_width=True)
```

#### 4. Create `app/views/order_lookup.py`

```python
"""Order Lookup view: parameterized drill-down into MART.FACT_ORDERS."""

from __future__ import annotations

import streamlit as st
from snowflake.snowpark import Session

from app.utils.queries import lookup_orders


def render(session: Session) -> None:
    """Render the Parameterized Order & Customer Lookup page."""
    st.header("Order & Customer Lookup")
    st.caption(
        "Drills down into `MART.FACT_ORDERS` using bound `?` SQL parameters (zero f-string SQL)."
    )

    col_t, col_v = st.columns([1, 2])
    with col_t:
        search_type = st.radio("Lookup By", options=["Order ID", "Customer ID"], horizontal=True)
    with col_v:
        placeholder = "e.g. ORD_000000001" if search_type == "Order ID" else "e.g. CUST_0000001"
        search_value = st.text_input("Identifier", placeholder=placeholder)

    if not search_value.strip():
        st.info("Enter an exact Order ID or Customer ID to inspect line items.")
        return

    df = lookup_orders(session, search_type=search_type, search_value=search_value)
    if df.empty:
        st.warning(f"No line items found for {search_type} = `{search_value.strip()}`.")
        return

    m1, m2, m3 = st.columns(3)
    m1.metric("Line Items Returned", f"{len(df):,}")
    m2.metric("Total Net Revenue", f"${df['NET_REVENUE'].sum():,.2f}")
    m3.metric("Total Net Margin", f"${df['NET_MARGIN'].sum():,.2f}")

    st.dataframe(df, use_container_width=True, hide_index=True)
```

---

### Step 2.4 `[CREATE FILE]` `app/streamlit_app.py`

```python
"""Main entry point for the E-Commerce Analytics & Operations Streamlit app."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on sys.path both locally and inside Streamlit in Snowflake
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st

from app.utils.snowflake_session import get_snowflake_session, is_running_in_sis
from app.views import cohort_retention, executive_kpi, inventory_alerts, order_lookup

st.set_page_config(
    page_title="Snowflake E-Commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

VIEWS = {
    "Executive KPIs": executive_kpi.render,
    "Cohort Retention": cohort_retention.render,
    "Inventory & SKU Health": inventory_alerts.render,
    "Order Lookup": order_lookup.render,
}


def main() -> None:
    session = get_snowflake_session()
    runtime_label = "Streamlit in Snowflake (SiS)" if is_running_in_sis() else "Local Dev"

    with st.sidebar:
        st.title("E-Commerce Platform")
        st.caption(f"Runtime: **{runtime_label}**")

        current_db = session.get_current_database() or "UNKNOWN"
        current_wh = session.get_current_warehouse() or "UNKNOWN"
        st.caption(f"DB: `{current_db}` | WH: `{current_wh}`")

        page = st.radio("Navigation", options=list(VIEWS.keys()))
        st.divider()
        if st.button("Clear Query Cache", use_container_width=True):
            st.cache_data.clear()
            st.toast("Cleared `@st.cache_data` cache.")

    VIEWS[page](session)


if __name__ == "__main__":
    main()
```

---

### Step 2.5 `[CREATE FILE]` Deployment Manifests & Local Secrets

#### 1. Create `snowflake.yml` (Container Runtime primary + Warehouse Runtime fallback)

```yaml
definition_version: 2

env:
  SNOWFLAKE_DATABASE: ECOMM_DEV
  SNOWFLAKE_SCHEMA: APP
  SNOWFLAKE_WAREHOUSE: ECOMM_APP_WH
  APP_NAME: ECOMM_ANALYTICS_APP

entities:
  ecomm_analytics_app:
    type: streamlit
    identifier:
      name: <% ctx.env.APP_NAME %>
      database: <% ctx.env.SNOWFLAKE_DATABASE %>
      schema: <% ctx.env.SNOWFLAKE_SCHEMA %>
    title: "E-Commerce Analytics & Operations"
    query_warehouse: <% ctx.env.SNOWFLAKE_WAREHOUSE %>
    # --- Container Runtime (SPCS) ---
    # If your 30-day trial region does not support SPCS compute pools,
    # comment out `compute_pool`, `runtime_name`, and `external_access_integrations`,
    # and add `- app/environment.yml` to `artifacts` to use Warehouse Runtime.
    compute_pool: ECOMM_APP_POOL
    runtime_name: SYSTEM$ST_CONTAINER_RUNTIME_PY3_11
    external_access_integrations:
      - ECOMM_PYPI_ACCESS
    main_file: app/streamlit_app.py
    artifacts:
      - app/streamlit_app.py
      - app/utils
      - app/views
      - pyproject.toml
```

#### 2. Create `app/environment.yml` (Warehouse Runtime fallback manifest)

```yaml
name: ecomm_sis_env
channels:
  - snowflake
dependencies:
  - python=3.11
  - streamlit
  - snowflake-snowpark-python
  - pandas
  - altair
```

#### 3. Create `.streamlit/secrets.toml` (LOCAL ONLY — gitignored)

Replace `<YOUR_...>` with your Snowflake account details so `st.connection("snowflake")` works locally:

```toml
[connections.snowflake]
account = "<YOUR_ACCOUNT_IDENTIFIER>"
user = "<YOUR_SNOWFLAKE_USERNAME>"
password = "<YOUR_SNOWFLAKE_PASSWORD>"
role = "ECOMM_APP_ROLE"
warehouse = "ECOMM_APP_WH"
database = "ECOMM_DEV"
schema = "MART"
```

*(If your trial account enforces browser MFA, set `authenticator = "externalbrowser"` and omit `password`.)*

---

### Step 2.6 `[CREATE FILE]` Unit Tests (`tests/`)

#### 1. Create `tests/test_data_generator.py`

```python
"""Unit tests for synthetic e-commerce dataset generation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.generate_data import generate_dataset


def test_generate_dataset_parquet_integrity(tmp_path: Path) -> None:
    counts = generate_dataset(rows=250, output_dir=tmp_path, fmt="parquet", seed=123)

    assert counts["order_items"] == 250
    assert counts["products"] == 250
    assert counts["customers"] == 1_000

    customers = pd.read_parquet(tmp_path / "customers" / "customers.parquet")
    products = pd.read_parquet(tmp_path / "products" / "products.parquet")
    orders = pd.read_parquet(tmp_path / "orders" / "orders_part_0000.parquet")
    items = pd.read_parquet(tmp_path / "order_items" / "order_items_part_0000.parquet")

    assert len(items) == 250
    assert customers["customer_id"].is_unique
    assert products["sku_id"].is_unique
    assert orders["order_id"].is_unique
    assert items["order_item_id"].is_unique

    # Referential integrity checks
    assert set(orders["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(items["order_id"]).issubset(set(orders["order_id"]))
    assert set(items["sku_id"]).issubset(set(products["sku_id"]))

    # Business constraints
    assert (products["unit_price"] > products["unit_cost"]).all()
    assert (products["stock_on_hand"] <= products["reorder_point"]).any()
    assert (items["quantity"] >= 1).all()


def test_generate_dataset_rejects_non_positive_rows(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        generate_dataset(rows=0, output_dir=tmp_path)
```

#### 2. Create `tests/test_queries.py`

```python
"""Unit tests for parameterized SQL query builders and KPI metric calculations."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.utils.queries import (
    build_in_placeholders,
    compute_kpi_metrics,
    compute_mom_growth,
    get_daily_revenue,
    get_inventory_health,
    lookup_orders,
)


def _mock_session_with_df(df: pd.DataFrame) -> MagicMock:
    session = MagicMock()
    session.sql.return_value.to_pandas.return_value = df
    return session


def test_build_in_placeholders() -> None:
    assert build_in_placeholders(1) == "?"
    assert build_in_placeholders(3) == "?, ?, ?"
    with pytest.raises(ValueError):
        build_in_placeholders(0)


def test_compute_kpi_metrics_uses_weighted_aov() -> None:
    df = pd.DataFrame(
        {
            "NET_REVENUE": [1000.0, 200.0],
            "ORDER_COUNT": [10, 1],
            "NET_MARGIN": [400.0, 80.0],
        }
    )
    metrics = compute_kpi_metrics(df)
    assert metrics["total_revenue"] == 1200.0
    assert metrics["total_orders"] == 11.0
    assert metrics["aov"] == round(1200.0 / 11.0, 2)
    assert metrics["net_margin"] == 480.0
    assert metrics["margin_pct"] == 40.0


def test_compute_kpi_metrics_empty_df() -> None:
    metrics = compute_kpi_metrics(pd.DataFrame())
    assert metrics == {
        "total_revenue": 0.0,
        "total_orders": 0.0,
        "aov": 0.0,
        "net_margin": 0.0,
        "margin_pct": 0.0,
    }


def test_compute_mom_growth() -> None:
    assert compute_mom_growth(125.0, 100.0) == 25.0
    assert compute_mom_growth(100.0, 0.0) is None


def test_get_daily_revenue_uses_bound_parameters() -> None:
    sample = pd.DataFrame(
        {
            "ORDER_DATE": ["2026-01-01"],
            "CATEGORY": ["Electronics"],
            "REGION": ["North America"],
            "ORDER_COUNT": [5],
            "UNIQUE_CUSTOMERS": [5],
            "UNITS_SOLD": [8],
            "GROSS_REVENUE": [500.0],
            "NET_REVENUE": [480.0],
            "TOTAL_COGS": [200.0],
            "NET_MARGIN": [280.0],
        }
    )
    mock_session = _mock_session_with_df(sample)
    get_daily_revenue.clear()

    res = get_daily_revenue(
        mock_session,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        categories=("Electronics", "Apparel"),
        regions=("North America",),
    )

    assert len(res) == 1
    called_sql, called_kwargs = mock_session.sql.call_args
    sql_text = called_sql[0]
    params = called_kwargs["params"]

    assert "Electronics" not in sql_text
    assert "North America" not in sql_text
    assert "?, ?" in sql_text
    assert params == ["2026-01-01", "2026-01-31", "Electronics", "Apparel", "North America"]


def test_lookup_orders_prevents_sql_injection() -> None:
    mock_session = _mock_session_with_df(pd.DataFrame())
    lookup_orders.clear()

    malicious_input = "ORD_1' OR 1=1 --"
    lookup_orders(mock_session, search_type="Order ID", search_value=malicious_input, limit=50)

    called_sql, called_kwargs = mock_session.sql.call_args
    sql_text = called_sql[0]
    params = called_kwargs["params"]

    assert malicious_input not in sql_text
    assert "WHERE ORDER_ID = ?" in sql_text
    assert params == [malicious_input, 50]


def test_get_inventory_health_only_at_risk_clause() -> None:
    mock_session = _mock_session_with_df(pd.DataFrame())
    get_inventory_health.clear()

    get_inventory_health(mock_session, categories=("Beauty",), only_at_risk=True)
    called_sql, called_kwargs = mock_session.sql.call_args
    assert "STOCKOUT_RISK_FLAG = TRUE" in called_sql[0]
    assert called_kwargs["params"] == ["Beauty"]
```

---

### Step 2.7 `[LOCAL TERMINAL + SNOWSIGHT UI]` Run Tests, Run Locally & Deploy to SiS

```bash
source .venv/bin/activate

# 1. Lint, format check, and run all unit tests
ruff check .
ruff format --check .
pytest -v

# 2. Launch locally using st.connection("snowflake")
streamlit run app/streamlit_app.py
```

Once the app works locally on `http://localhost:8501`, deploy it to **`ECOMM_DEV.APP`** via Snowflake CLI:

```bash
snow streamlit deploy --replace -c dev --database ECOMM_DEV --schema APP --role ECOMM_CICD_ROLE
```

*(Note: If `snow streamlit deploy` reports that Container Runtime compute pools are unavailable in your trial region, comment out `compute_pool`, `runtime_name`, and `external_access_integrations` in `snowflake.yml`, add `- app/environment.yml` under `artifacts`, and re-run `snow streamlit deploy --replace`.)*

---

### Step 2.8 `[VERIFY]` Phase 2 Definition of Done

1. `pytest -v` passes **100%** and `ruff check .` exits `0`.
2. Open **Snowsight -> Projects -> Streamlit -> ECOMM_ANALYTICS_APP** (in `ECOMM_DEV.APP`) and confirm all 4 pages render cleanly.
3. **Verify `@st.cache_data` caching:**
   - Open **Snowsight -> Monitoring -> Query History** filtered by `WAREHOUSE = ECOMM_APP_WH`.
   - In the app, change a category filter once (1 new SQL query appears in Query History), then toggle back to the original category selection.
   - Confirm **zero new queries** execute on the repeated selection (served directly from `@st.cache_data`).

---
## Phase 3: Pillar 3 — CI/CD with GitHub Actions & RSA Key-Pair Auth

**Goal:** Eliminate passwords and interactive MFA from automation by configuring an encrypted 2048-bit RSA key pair for `SVC_GITHUB_CICD` (`TYPE = SERVICE`) and automating PR checks (`ci.yml`), Dev deploys (`deploy-dev.yml`), and Prod deploys (`deploy-prod.yml`).

---

### Step 3.1 `[LOCAL TERMINAL]` Generate Encrypted RSA 2048-Bit Key Pair

Store keys **outside** your Git repository (e.g., `~/.snowflake/keys/`):

```bash
mkdir -p ~/.snowflake/keys
chmod 700 ~/.snowflake/keys

# 1. Generate encrypted PKCS#8 private key (enter a strong passphrase and save it!)
openssl genrsa 2048 | openssl pkcs8 -topk8 -v2 aes-256-cbc -inform PEM \
  -out ~/.snowflake/keys/ecomm_cicd_rsa_key.p8

chmod 600 ~/.snowflake/keys/ecomm_cicd_rsa_key.p8

# 2. Extract the matching public key (prompts for your passphrase)
openssl rsa -in ~/.snowflake/keys/ecomm_cicd_rsa_key.p8 -pubout \
  -out ~/.snowflake/keys/ecomm_cicd_rsa_key.pub

# 3. Print the single-line public key body (strips BEGIN/END headers & newlines)
grep -v "PUBLIC KEY" ~/.snowflake/keys/ecomm_cicd_rsa_key.pub | tr -d '\n'; echo
```

Copy the single-line string printed by command #3.

---

### Step 3.2 `[SNOWSIGHT UI + LOCAL TERMINAL]` Attach Public Key & Smoke-Test Auth

1. **`[SNOWSIGHT UI]`** Run as `ACCOUNTADMIN` or `SECURITYADMIN`:

```sql
USE ROLE ACCOUNTADMIN;

ALTER USER SVC_GITHUB_CICD
    SET RSA_PUBLIC_KEY = '<PASTE_SINGLE_LINE_PUBLIC_KEY_HERE>';

DESC USER SVC_GITHUB_CICD;
```

2. **`[LOCAL TERMINAL]`** Smoke-test key-pair authentication using temporary connection mode (`-x`):

```bash
source .venv/bin/activate

SNOWFLAKE_ACCOUNT="<YOUR_ACCOUNT_IDENTIFIER>" \
SNOWFLAKE_USER="SVC_GITHUB_CICD" \
SNOWFLAKE_ROLE="ECOMM_CICD_ROLE" \
SNOWFLAKE_WAREHOUSE="ECOMM_ETL_WH" \
SNOWFLAKE_DATABASE="ECOMM_DEV" \
SNOWFLAKE_SCHEMA="MART" \
SNOWFLAKE_AUTHENTICATOR="SNOWFLAKE_JWT" \
SNOWFLAKE_PRIVATE_KEY_FILE="$HOME/.snowflake/keys/ecomm_cicd_rsa_key.p8" \
PRIVATE_KEY_PASSPHRASE="<YOUR_KEY_PASSPHRASE>" \
snow sql -x -q "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_DATABASE();"
```

Confirm it prints `SVC_GITHUB_CICD | ECOMM_CICD_ROLE | ECOMM_DEV`.

---

### Step 3.3 `[GITHUB UI]` Configure Environments & Secrets

In your GitHub repository -> **Settings -> Environments**:

1. Create two environments: **`dev`** and **`prod`** (on `prod`, you can optionally enable *Required reviewers* so merges to `main` pause for a click approval).
2. In **both** `dev` and `prod` environments (or as repository-level secrets if shared, though environment secrets are cleaner), add these 5 secrets:

| Secret Name | Value |
|---|---|
| `SNOWFLAKE_ACCOUNT` | Your account identifier (`orgname-accountname`) |
| `SNOWFLAKE_USER` | `SVC_GITHUB_CICD` |
| `SNOWFLAKE_ROLE` | `ECOMM_CICD_ROLE` |
| `SNOWFLAKE_PRIVATE_KEY` | Full contents of `~/.snowflake/keys/ecomm_cicd_rsa_key.p8` (including `-----BEGIN ENCRYPTED PRIVATE KEY-----` and `-----END ENCRYPTED PRIVATE KEY-----`) |
| `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` | The passphrase you entered in Step 3.1 |

---

### Step 3.4 `[CREATE FILE]` GitHub Actions Workflows (`.github/workflows/`)

#### 1. Create `.github/workflows/ci.yml`

```yaml
name: CI Checks

on:
  pull_request:
    branches:
      - dev
      - main

jobs:
  quality-and-tests:
    runs-on: ubuntu-latest

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install uv and dependencies
        run: |
          pip install uv
          uv pip install --system -e ".[dev]"

      - name: Run Ruff lint
        run: ruff check .

      - name: Run Ruff format check
        run: ruff format --check .

      - name: Run Pytest unit tests
        run: pytest -v

      - name: SQL & repo safety guardrails
        run: |
          python - <<'EOF'
          from pathlib import Path
          import sys

          bootstrap = Path("sql/01_bootstrap_account.sql").read_text()
          if "AUTO_SUSPEND = 60" not in bootstrap:
              print("ERROR: AUTO_SUSPEND = 60 missing in sql/01_bootstrap_account.sql")
              sys.exit(1)

          for forbidden in [".env", ".streamlit/secrets.toml"]:
              if Path(forbidden).exists():
                  print(f"ERROR: Forbidden secret file tracked in repo: {forbidden}")
                  sys.exit(1)

          p8_files = list(Path(".").rglob("*.p8"))
          if p8_files:
              print(f"ERROR: Private key files found in repo: {p8_files}")
              sys.exit(1)

          print("All SQL and security guardrails passed.")
          EOF
```

#### 2. Create `.github/workflows/deploy-dev.yml`

```yaml
name: Deploy to ECOMM_DEV

on:
  push:
    branches:
      - dev

jobs:
  deploy-dev:
    runs-on: ubuntu-latest
    environment: dev

    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
      SNOWFLAKE_WAREHOUSE: ECOMM_ETL_WH
      SNOWFLAKE_DATABASE: ECOMM_DEV
      SNOWFLAKE_SCHEMA: APP
      SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
      SNOWFLAKE_PRIVATE_KEY_FILE: ${{ runner.temp }}/rsa_key.p8
      PRIVATE_KEY_PASSPHRASE: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE }}

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies and Snowflake CLI
        run: |
          pip install uv
          uv pip install --system -e ".[dev]"

      - name: Run lint and unit tests
        run: |
          ruff check .
          pytest -v

      - name: Write temporary RSA private key
        run: |
          printf '%s\n' "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > "${{ runner.temp }}/rsa_key.p8"
          chmod 600 "${{ runner.temp }}/rsa_key.p8"

      - name: Apply idempotent SQL migrations to ECOMM_DEV
        run: |
          snow connection test -x
          snow sql -x -f sql/02_schemas_and_stages.sql -D "db=ECOMM_DEV" --enable-templating JINJA
          snow sql -x -f sql/03_raw_tables.sql -D "db=ECOMM_DEV" --enable-templating JINJA

      - name: Deploy Streamlit app to ECOMM_DEV.APP
        run: |
          snow streamlit deploy --replace -x

      - name: Clean up temporary private key
        if: always()
        run: rm -f "${{ runner.temp }}/rsa_key.p8"
```

#### 3. Create `.github/workflows/deploy-prod.yml`

```yaml
name: Deploy to ECOMM_PROD

on:
  push:
    branches:
      - main

jobs:
  deploy-prod:
    runs-on: ubuntu-latest
    environment: prod

    env:
      SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
      SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
      SNOWFLAKE_ROLE: ${{ secrets.SNOWFLAKE_ROLE }}
      SNOWFLAKE_WAREHOUSE: ECOMM_ETL_WH
      SNOWFLAKE_DATABASE: ECOMM_PROD
      SNOWFLAKE_SCHEMA: APP
      SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
      SNOWFLAKE_PRIVATE_KEY_FILE: ${{ runner.temp }}/rsa_key.p8
      PRIVATE_KEY_PASSPHRASE: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE }}

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies and Snowflake CLI
        run: |
          pip install uv
          uv pip install --system -e ".[dev]"

      - name: Run quality gate
        run: |
          ruff check .
          ruff format --check .
          pytest -v

      - name: Write temporary RSA private key
        run: |
          printf '%s\n' "${{ secrets.SNOWFLAKE_PRIVATE_KEY }}" > "${{ runner.temp }}/rsa_key.p8"
          chmod 600 "${{ runner.temp }}/rsa_key.p8"

      - name: Apply idempotent SQL migrations to ECOMM_PROD
        run: |
          snow connection test -x
          snow sql -x -f sql/02_schemas_and_stages.sql -D "db=ECOMM_PROD" --enable-templating JINJA
          snow sql -x -f sql/03_raw_tables.sql -D "db=ECOMM_PROD" --enable-templating JINJA

      - name: Deploy Streamlit app to ECOMM_PROD.APP
        run: |
          snow streamlit deploy --replace -x

      - name: Clean up temporary private key
        if: always()
        run: rm -f "${{ runner.temp }}/rsa_key.p8"
```

---

### Step 3.5 `[LOCAL TERMINAL + GITHUB UI]` Commit, Push & Seed `ECOMM_PROD`

1. Format your code cleanly and inspect `git status` (verify **zero** `.p8`, `.env`, `.streamlit/secrets.toml`, or `data/raw/` files appear!):

```bash
source .venv/bin/activate
ruff format .
ruff check .
pytest -v
git status
```

2. Commit and push to `dev`:

```bash
git add .
git commit -m "feat: scaffold snowflake e-commerce platform, SiS app, tests, and CI/CD"
git push -u origin dev
```

3. Watch **GitHub -> Actions -> Deploy to ECOMM_DEV** turn green!
4. Open a **Pull Request** from `dev` -> `main` on GitHub:
   - Confirm **`CI Checks`** (`ci.yml`) runs and turns green.
   - Merge the PR into `main`.
   - Confirm **`Deploy to ECOMM_PROD`** (`deploy-prod.yml`) runs and deploys `ECOMM_PROD.APP.ECOMM_ANALYTICS_APP`.
5. Populate `ECOMM_PROD.RAW` and `ECOMM_PROD.MART` once from your local terminal so the production Streamlit app has data to display:

```bash
snow stage copy data/raw/customers "@ECOMM_PROD.RAW.ECOMM_STAGE/customers" --overwrite -c dev
snow stage copy data/raw/products "@ECOMM_PROD.RAW.ECOMM_STAGE/products" --overwrite -c dev
snow stage copy data/raw/orders "@ECOMM_PROD.RAW.ECOMM_STAGE/orders" --overwrite -c dev
snow stage copy data/raw/order_items "@ECOMM_PROD.RAW.ECOMM_STAGE/order_items" --overwrite -c dev

snow sql -c dev -f sql/04_load_copy_into.sql -D "db=ECOMM_PROD" --enable-templating JINJA
python scripts/run_snowpark_etl.py --database ECOMM_PROD --connection dev
```

---

### Step 3.6 `[VERIFY]` Phase 3 Definition of Done

1. Both `Deploy to ECOMM_DEV` and `Deploy to ECOMM_PROD` GitHub Actions workflows succeeded with zero passwords stored.
2. Opening `ECOMM_PROD.APP.ECOMM_ANALYTICS_APP` in Snowsight loads live charts cleanly.

---

## Phase 4: Pillar 4 — SQL Performance Tuning & Cost Proof

**Goal:** Prove micro-partition pruning on a **5,000,000-row** `FACT_ORDERS` table by measuring `Partitions scanned`, `Bytes scanned`, and execution time before vs. after sorted loads/clustering with Result Cache disabled—then documenting everything in `docs/performance.md`.

---

### Step 4.1 `[LOCAL TERMINAL]` Load 5,000,000 Unsorted Rows into `ECOMM_DEV`

```bash
source .venv/bin/activate

# 1. Generate 5M unsorted line items (~150-250 MB Parquet, takes ~15-30s)
python scripts/generate_data.py --rows 5000000 --unsorted --output-dir data/raw --format parquet

# 2. Upload to ECOMM_DEV stage
snow stage copy data/raw/customers "@ECOMM_DEV.RAW.ECOMM_STAGE/customers" --overwrite -c dev
snow stage copy data/raw/products "@ECOMM_DEV.RAW.ECOMM_STAGE/products" --overwrite -c dev
snow stage copy data/raw/orders "@ECOMM_DEV.RAW.ECOMM_STAGE/orders" --overwrite -c dev
snow stage copy data/raw/order_items "@ECOMM_DEV.RAW.ECOMM_STAGE/order_items" --overwrite -c dev

# 3. Bulk load RAW tables and build UNSORTED MART.FACT_ORDERS (no --sort-fact-orders flag!)
snow sql -c dev -f sql/04_load_copy_into.sql -D "db=ECOMM_DEV" --enable-templating JINJA
python scripts/run_snowpark_etl.py --database ECOMM_DEV --connection dev
```

---

### Step 4.2 `[CREATE FILE]` `sql/05_mart_and_clustering.sql`

```sql
-- =============================================================================
-- 05_mart_and_clustering.sql
-- Purpose : Micro-partition pruning & Result Cache benchmark on 5M-row FACT_ORDERS
-- Run In  : Snowsight Worksheet (so you can inspect Query Profile visually)
-- =============================================================================

USE ROLE ECOMM_CICD_ROLE;
USE WAREHOUSE ECOMM_APP_WH;
USE DATABASE ECOMM_DEV;
USE SCHEMA MART;

-- -----------------------------------------------------------------------------
-- 1. Disable 24-hour Result Cache for fair compute & I/O benchmarking
-- -----------------------------------------------------------------------------
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

-- Inspect baseline clustering depth/overlap on unsorted FACT_ORDERS
SELECT SYSTEM$CLUSTERING_INFORMATION('ECOMM_DEV.MART.FACT_ORDERS', '(ORDER_DATE, CATEGORY)');

-- -----------------------------------------------------------------------------
-- 2. BASELINE QUERY (Unsorted Table — expect full or near-full partition scan)
--    Capture Query Profile Screenshot #1: Partitions scanned vs Partitions total
-- -----------------------------------------------------------------------------
SELECT
    ORDER_DATE,
    CATEGORY,
    COUNT(DISTINCT ORDER_ID) AS ORDER_COUNT,
    ROUND(SUM(NET_REVENUE), 2) AS NET_REVENUE,
    ROUND(SUM(NET_MARGIN), 2) AS NET_MARGIN
FROM ECOMM_DEV.MART.FACT_ORDERS
WHERE ORDER_DATE BETWEEN '2026-06-01' AND '2026-06-07'
  AND CATEGORY = 'Electronics'
GROUP BY ORDER_DATE, CATEGORY
ORDER BY ORDER_DATE;

-- -----------------------------------------------------------------------------
-- 3. OPTIMIZATION A: Zero-Cost Batch Sorted Load (ORDER BY on CTAS / ETL write)
--    For tables < 1 TB, sorting on batch write gives pruning for $0 serverless cost!
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE ECOMM_DEV.MART.FACT_ORDERS_SORTED AS
SELECT *
FROM ECOMM_DEV.MART.FACT_ORDERS
ORDER BY ORDER_DATE, CATEGORY;

-- Inspect improved clustering depth & overlap (~1.0 depth, ~0.0 overlap)
SELECT SYSTEM$CLUSTERING_INFORMATION('ECOMM_DEV.MART.FACT_ORDERS_SORTED', '(ORDER_DATE, CATEGORY)');

-- -----------------------------------------------------------------------------
-- 4. PRUNED QUERY (Sorted Table — expect dramatic partition & byte reduction)
--    Capture Query Profile Screenshot #2: Partitions scanned vs Partitions total
-- -----------------------------------------------------------------------------
SELECT
    ORDER_DATE,
    CATEGORY,
    COUNT(DISTINCT ORDER_ID) AS ORDER_COUNT,
    ROUND(SUM(NET_REVENUE), 2) AS NET_REVENUE,
    ROUND(SUM(NET_MARGIN), 2) AS NET_MARGIN
FROM ECOMM_DEV.MART.FACT_ORDERS_SORTED
WHERE ORDER_DATE BETWEEN '2026-06-01' AND '2026-06-07'
  AND CATEGORY = 'Electronics'
GROUP BY ORDER_DATE, CATEGORY
ORDER BY ORDER_DATE;

-- -----------------------------------------------------------------------------
-- 5. OPTIMIZATION B (Demo Only): Explicit Clustering Key + Immediate Cost Guard
--    Automatic clustering consumes serverless credits on ongoing DML.
--    We demonstrate the syntax, then immediately SUSPEND RECLUSTER to protect trial credits.
-- -----------------------------------------------------------------------------
ALTER TABLE ECOMM_DEV.MART.FACT_ORDERS_SORTED
    CLUSTER BY (ORDER_DATE, CATEGORY);

ALTER TABLE ECOMM_DEV.MART.FACT_ORDERS_SORTED
    SUSPEND RECLUSTER;

-- -----------------------------------------------------------------------------
-- 6. RESULT CACHE PROOF: Re-enable 24h Result Cache and run identical query twice
--    Run 2 returns from Cloud Services Result Cache (<100ms, 0 warehouse compute!)
--    Capture Query Profile Screenshot #3 on the second execution.
-- -----------------------------------------------------------------------------
ALTER SESSION SET USE_CACHED_RESULT = TRUE;

SELECT
    ORDER_DATE,
    CATEGORY,
    COUNT(DISTINCT ORDER_ID) AS ORDER_COUNT,
    ROUND(SUM(NET_REVENUE), 2) AS NET_REVENUE,
    ROUND(SUM(NET_MARGIN), 2) AS NET_MARGIN
FROM ECOMM_DEV.MART.FACT_ORDERS_SORTED
WHERE ORDER_DATE BETWEEN '2026-06-01' AND '2026-06-07'
  AND CATEGORY = 'Electronics'
GROUP BY ORDER_DATE, CATEGORY
ORDER BY ORDER_DATE;
```

---

### Step 4.3 `[SNOWSIGHT UI]` Run the Benchmark & Capture Query Profile Screenshots

1. Open `sql/05_mart_and_clustering.sql` in a **Snowsight Worksheet**.
2. Execute **Section 1 & Section 2** (`ALTER SESSION SET USE_CACHED_RESULT = FALSE`, `SYSTEM$CLUSTERING_INFORMATION`, and the Baseline Query).
3. Click the **Query ID -> View Query Profile** on the Baseline Query and record:
   - `Partitions scanned` / `Partitions total`
   - `Bytes scanned`
   - `Total execution time`
   - Take a screenshot and save it as `docs/images/query_profile_baseline.png`.
4. Execute **Section 3 & Section 4** (creates `FACT_ORDERS_SORTED` ordered by `(ORDER_DATE, CATEGORY)`, checks `SYSTEM$CLUSTERING_INFORMATION`, and runs the Pruned Query).
5. Open **Query Profile** on the Pruned Query, record the new numbers, and save a screenshot as `docs/images/query_profile_pruned.png`.
6. Execute **Section 5** (adds `CLUSTER BY` and immediately runs `SUSPEND RECLUSTER` so zero background credits are spent).
7. Execute **Section 6** **twice** in a row; open **Query Profile** on the **second** execution (shows `QUERY RESULT REUSE`, `0` warehouse compute) and save `docs/images/query_profile_cached.png`.

---

### Step 4.4 `[CREATE FILE]` `docs/performance.md`

Fill in the `<RECORDED_...>` placeholders below with your actual Snowsight Query Profile numbers:

~~~~markdown
# Snowflake Micro-Partition Pruning & Cost Optimization Benchmark

## 1. Benchmark Setup

* **Table:** `ECOMM_DEV.MART.FACT_ORDERS` (~5,000,000 line-item rows, intentionally ingested unsorted)
* **Warehouse:** `ECOMM_APP_WH` (`X-SMALL`, `AUTO_SUSPEND = 60`)
* **Workload:** Selective 7-day window + single-category aggregation (`ORDER_DATE BETWEEN '2026-06-01' AND '2026-06-07' AND CATEGORY = 'Electronics'`)
* **Control:** `ALTER SESSION SET USE_CACHED_RESULT = FALSE` during Baseline and Sorted runs so Snowflake's 24-hour Result Cache does not mask storage I/O.

---

## 2. Before / After Results

| Metric | Baseline (Unsorted `FACT_ORDERS`) | Optimized (`ORDER BY ORDER_DATE, CATEGORY`) | Cached (`USE_CACHED_RESULT = TRUE`, 2nd run) |
|---|---|---|---|
| **Partitions Scanned** | `<RECORDED_BASE_PARTITIONS_SCANNED>` | `<RECORDED_OPT_PARTITIONS_SCANNED>` | `0 (Result Reuse)` |
| **Total Partitions** | `<RECORDED_BASE_PARTITIONS_TOTAL>` | `<RECORDED_OPT_PARTITIONS_TOTAL>` | `0` |
| **Partitions Pruned (%)** | `<CALCULATED_% >` | `<CALCULATED_% >` | `100%` |
| **Bytes Scanned** | `<RECORDED_BASE_BYTES>` | `<RECORDED_OPT_BYTES>` | `0 B` |
| **Execution Time** | `<RECORDED_BASE_TIME>` | `<RECORDED_OPT_TIME>` | `<RECORDED_CACHED_TIME>` |

---

## 3. Clustering Health (`SYSTEM$CLUSTERING_INFORMATION`)

* **Baseline (`FACT_ORDERS`):**
  * `average_overlaps`: `<RECORDED_BASE_OVERLAP>`
  * `average_depth`: `<RECORDED_BASE_DEPTH>`
* **Sorted (`FACT_ORDERS_SORTED`):**
  * `average_overlaps`: `<RECORDED_OPT_OVERLAP>`
  * `average_depth`: `<RECORDED_OPT_DEPTH>`

---

## 4. Query Profile Evidence

### Baseline (Full / Near-Full Scan)
![Baseline Query Profile](images/query_profile_baseline.png)

### Sorted / Clustered (Pruned Scan)
![Pruned Query Profile](images/query_profile_pruned.png)

### 24-Hour Result Cache Reuse
![Result Cache Query Profile](images/query_profile_cached.png)

---

## 5. Architectural Cost Rationale

1. **Why Batch Sorting (`ORDER BY`) Beats Automatic Clustering Below ~1 TB:**
   Snowflake's automatic clustering runs background serverless compute and bills credits continuously as data mutates. For analytical tables rebuilt or appended in batch (and under ~1 TB), sorting by `(ORDER_DATE, CATEGORY)` inside the ETL write achieves near-ideal micro-partition min/max metadata (`average_depth ≈ 1.0`) at **zero** ongoing serverless reclustering cost.
2. **Why `DIM_PRODUCTS` and `DIM_CUSTOMERS` Are Never Auto-Clustered:**
   Small dimension tables (`DIM_PRODUCTS` = 250 rows, fitting inside a single 16 MB micro-partition) cannot be pruned further—enabling `CLUSTER BY` on them adds metadata/reclustering overhead with zero scan reduction.
3. **Two-Layer Defense Against Unnecessary Warehouse Wakeups:**
   Pre-aggregated `MART.AGG_DAILY_REVENUE` + Streamlit `@st.cache_data(ttl=600)` prevents interactive filter toggles from even hitting `FACT_ORDERS`, while Snowflake's 24-hour Result Cache serves identical queries from Cloud Services without waking `ECOMM_APP_WH`.
~~~~

---

### Step 4.5 `[VERIFY]` Phase 4 Definition of Done

1. `docs/performance.md` contains your real measured `Partitions scanned`, `Total partitions`, `Bytes scanned`, and execution times plus all 3 screenshots in `docs/images/`.
2. `ECOMM_DEV.MART.FACT_ORDERS_SORTED` has automatic reclustering suspended (`SUSPEND RECLUSTER`) so trial credits are protected.

---

## Phase 5: Polish & Interview Readiness

**Goal:** Write a crisp, recruiter-ready `README.md`, capture a short dashboard demo, and commit your final polish to `dev` -> `main`.

---

### Step 5.1 `[CREATE FILE]` `README.md`

~~~~markdown
# E-Commerce Analytics & Operations Platform (Snowflake + SiS)

A production-style, end-to-end e-commerce analytics platform built on **Snowflake**, **Snowpark Python**, **Streamlit in Snowflake (SiS)**, and **GitHub Actions CI/CD** with **RSA Key-Pair Authentication** and documented **micro-partition pruning benchmarks**.

---

## Architecture

```text
[Local Dev / Feature Branch]
         |
         v (push to `dev` or PR merge to `main`)
[GitHub Actions CI/CD] --(ruff + pytest + RSA Key-Pair Auth)--> [Snowflake CLI (`snow`)]
                                                                       |
              +--------------------------------------------------------+
              |                                                        |
              v                                                        v
   [SQL DDL & Ingestion]                                    [Streamlit in Snowflake]
   - Warehouses (ETL vs App)                                - Container Runtime (`pyproject.toml`)
   - RBAC Roles & Grants                                    - Fallback (`environment.yml`)
   - Internal Stage -> COPY INTO -> RAW                     - `@st.cache_data(ttl=600)`
   - Snowpark Transforms -> MART                            - Reads ONLY from `MART` schema
   - Clustered `FACT_ORDERS` (Pruned Scans)
```

---

## Key Engineering Highlights

* **Workload & Cost Isolation:** Separate `ECOMM_ETL_WH` (batch ingestion/Snowpark) and `ECOMM_APP_WH` (interactive Streamlit), both `X-SMALL` with `AUTO_SUSPEND = 60` and `AUTO_RESUME = TRUE`.
* **Least-Privilege RBAC:** `ECOMM_APP_ROLE` holds `SELECT` on `MART.*` (via `FUTURE` grants) and **zero access** to `RAW.*`. `SVC_GITHUB_CICD` uses `TYPE = SERVICE` + encrypted 2048-bit RSA key-pair auth.
* **Dual-Runtime Streamlit Portability:** Exact same codebase runs locally via `st.connection("snowflake")` and inside SiS via `get_active_session()`. All queries use `@st.cache_data(ttl=600)` and bound `?` SQL parameters.
* **Container Runtime First:** Configured for SiS Container Runtime (`pyproject.toml` + `SYSTEM$ST_CONTAINER_RUNTIME_PY3_11`), with `app/environment.yml` maintained for Warehouse Runtime compatibility.
* **Proven SQL Pruning:** Sorted batch loads on 5M-row `FACT_ORDERS` eliminate overlapping micro-partitions without paying serverless auto-clustering overhead. See **[docs/performance.md](docs/performance.md)**.

---

## Dashboard Views

1. **Executive KPIs:** Net revenue, completed orders, weighted AOV, net margin with prior-period deltas, daily trend, and category/region breakdowns (`MART.AGG_DAILY_REVENUE`).
2. **Cohort Retention:** Monthly signup repeat-purchase heatmap (`MART.AGG_COHORT_RETENTION`).
3. **Inventory & SKU Health:** 30-day sell-through velocity, days of supply, stockout risk alerts, and top/bottom margin SKUs (`MART.DIM_PRODUCTS`).
4. **Order & Customer Lookup:** Parameterized line-item drill-down (`MART.FACT_ORDERS`).

![Dashboard Preview](docs/images/dashboard_preview.png)

---

## Local Quickstart

```bash
# 1. Create Python 3.11 environment
uv venv --python 3.11
source .venv/bin/activate
uv pip install -e ".[dev]"

# 2. Generate synthetic Parquet data & run unit tests
python scripts/generate_data.py --rows 50000
ruff check . && pytest -v

# 3. Configure .streamlit/secrets.toml (never commit!) and launch locally
streamlit run app/streamlit_app.py
```

---

## CI/CD Workflow

* **Pull Requests -> `dev` / `main`:** `.github/workflows/ci.yml` runs `ruff check`, `ruff format --check`, `pytest`, and repo/SQL guardrails.
* **Push to `dev`:** `.github/workflows/deploy-dev.yml` authenticates via RSA key-pair (`SNOWFLAKE_JWT`), applies idempotent DDL to `ECOMM_DEV`, and deploys `ECOMM_DEV.APP.ECOMM_ANALYTICS_APP`.
* **Merge to `main`:** `.github/workflows/deploy-prod.yml` runs quality gates and deploys to `ECOMM_PROD.APP.ECOMM_ANALYTICS_APP`.
~~~~

---

### Step 5.2 `[HUMAN]` Capture Demo Screenshot / GIF & Push Final Commit

1. Open your live SiS app in Snowsight (`ECOMM_PROD.APP.ECOMM_ANALYTICS_APP`).
2. Capture a clean screenshot of the Executive KPI dashboard and save it as `docs/images/dashboard_preview.png` (and optionally record a 1–2 minute silent GIF/Loom walking through all 4 views).
3. Commit and push to `dev`, then merge a PR into `main`:

```bash
source .venv/bin/activate
ruff check . && ruff format --check . && pytest -v
git add docs/ README.md sql/05_mart_and_clustering.sql
git commit -m "docs: add 5M-row pruning benchmark results and recruiter-ready README"
git push origin dev
```

Open a PR `dev` -> `main` on GitHub and merge once green.

---

### Step 5.3 Interview Talking Points Cheat Sheet

1. **Container Runtime vs. Warehouse Runtime:** Container Runtime (SPCS) uses `pyproject.toml` / PyPI packages, supports modern Streamlit features, and shares a single container across concurrent viewers. Warehouse Runtime (`environment.yml` + Snowflake Anaconda channel) spins up per-viewer Python runtimes—great as a zero-SPCS fallback on restricted regions.
2. **Why `TYPE = SERVICE` + RSA Key-Pair Auth:** Snowflake enforces MFA on interactive human users. CI/CD pipelines use `TYPE = SERVICE` users authenticated via 2048-bit RSA keys (`SNOWFLAKE_JWT`) so automation never breaks on MFA prompts or expiring passwords.
3. **Why Separate ETL and App Warehouses:** Heavy `COPY INTO` or Snowpark batch merges on `ECOMM_ETL_WH` never queue or degrade interactive executive dashboard queries on `ECOMM_APP_WH`.
4. **Why Clustering Isn't Free:** Background automatic reclustering burns serverless credits. Under ~1 TB, sorting on batch write (`ORDER BY`) yields free micro-partition pruning; reserve `CLUSTER BY` for multi-terabyte continuously appended fact tables.
5. **E-Commerce Postgres/MySQL -> Snowflake Migration Pattern:** Extract incrementally or via CDC into immutable `RAW` landing tables using staged bulk `COPY INTO` (never row-by-row `INSERT`), then transform via Snowpark into governed `MART` tables consumed by `ECOMM_APP_ROLE`.
