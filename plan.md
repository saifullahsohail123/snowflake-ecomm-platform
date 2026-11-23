# Project Plan: E-Commerce Analytics & Operations Platform (Snowflake + SiS)

> **Purpose:** Build a production-grade, end-to-end e-commerce analytics application on **Snowflake** and **Streamlit in Snowflake (SiS)** with automated **GitHub Actions CI/CD** and documented **SQL cost/performance optimization**.
> **Dual Audience:** Written so a **human developer** can follow progress at a glance and an **AI coding assistant** can implement each phase deterministically without guessing conventions.

---

## 0. Rules & Guardrails (For AI Agents & Contributors)

Follow these non-negotiable constraints across every phase:

1. **Zero Secrets in Git:** Never commit `.p8` private keys, passphrases, passwords, or Snowflake account identifiers in plaintext. Always read credentials from environment variables or GitHub Secrets.
2. **Environment Isolation:** Never deploy experimental SQL or Streamlit changes directly to `ECOMM_PROD`. All work targets `ECOMM_DEV` first via the `dev` branch.
3. **No OLTP Anti-Patterns:** Never write single-row `INSERT` or `UPDATE` loops against Snowflake. Use staged bulk loads (`COPY INTO`) or Snowpark DataFrame batch writes.
4. **Parameterized Queries Only:** All user-filtered SQL in Streamlit must use bound parameters (`?` / `:param` or Snowpark DataFrame `.filter()`), never f-string SQL interpolation.
5. **Cost Safety:** Every warehouse must specify `AUTO_SUSPEND = 60` and `AUTO_RESUME = TRUE`. Never leave auto-clustering enabled on small lookup tables.
6. **Runtime Portability:** Write the Streamlit session layer so the exact same app code runs locally (`st.connection("snowflake")`) and inside SiS (`get_active_session()`).
7. **Verification Gate:** Do not mark a phase complete until every command/check in its **Definition of Done** passes.

---

## 1. Architecture Overview

~~~text
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
~~~

### Tech Stack
* **Data Warehouse:** Snowflake (30-day trial account)
* **Data Transformation & App API:** Python 3.11+, Snowpark (`snowflake-snowpark-python`)
* **Frontend:** Streamlit in Snowflake (Container Runtime primary; Warehouse Runtime documented)
* **CI/CD & CLI:** GitHub Actions, Snowflake CLI (`snow`), RSA Key-Pair Auth (`TYPE = SERVICE` user)
* **Quality & Testing:** `ruff` (lint/format), `pytest` (unit + transformation tests)

---

## 2. Target Repository Structure

AI agents must follow this exact directory layout:

~~~text
snowflake-ecomm-platform/
├── plan.md                         # This master plan
├── README.md                       # Architecture overview, setup steps, and demo guide
├── pyproject.toml                  # Python deps (Container Runtime + dev tools: ruff, pytest)
├── snowflake.yml                   # Snowflake CLI project definition (dev/prod targets)
├── .env.example                    # Template for local connection env vars (never commit .env)
├── .gitignore                      # Ignores .env, *.p8, *.pub, __pycache__, data/raw/
├── .github/
│   └── workflows/
│       ├── ci.yml                  # PR checks: ruff, pytest, SQL syntax sanity check
│       ├── deploy-dev.yml          # Push to `dev` -> deploys SQL + SiS to ECOMM_DEV
│       └── deploy-prod.yml         # Merge to `main` -> deploys SQL + SiS to ECOMM_PROD
├── scripts/
│   ├── generate_data.py            # Generates realistic e-commerce CSV/Parquet data (scalable to 5M+ rows)
│   └── run_snowpark_etl.py         # Snowpark Session API pipeline: RAW -> MART tables
├── sql/
│   ├── 01_bootstrap_account.sql    # [One-time ACCOUNTADMIN] Warehouses, DBs, Roles, Service User
│   ├── 02_schemas_and_stages.sql   # RAW, MART, APP schemas + internal stages + file formats
│   ├── 03_raw_tables.sql           # DDL for RAW landing tables
│   ├── 04_load_copy_into.sql       # COPY INTO statements with ON_ERROR handling
│   └── 05_mart_and_clustering.sql  # Analytical tables/views + clustering experiment DDL
├── app/
│   ├── streamlit_app.py            # Main SiS entry point (navigation & layout)
│   ├── environment.yml             # Warehouse Runtime compatibility manifest (reference)
│   ├── utils/
│   │   ├── snowflake_session.py    # Unified get_active_session() / local connection helper
│   │   └── queries.py              # Cached, parameterized queries against MART schema
│   └── views/
│       ├── executive_kpi.py        # Revenue, AOV, conversion, MoM growth
│       ├── cohort_retention.py     # Monthly customer retention matrix
│       ├── inventory_alerts.py     # SKU velocity, stockout risk, margin analysis
│       └── order_lookup.py         # Parameterized order & customer search
├── tests/
│   ├── test_data_generator.py      # Validates schema and edge cases of generated data
│   └── test_queries.py             # Validates SQL builders and metric calculations (mocked session)
└── docs/
    └── performance.md              # Query Profile screenshots + before/after pruning benchmarks
~~~

---

## 3. Snowflake Object & RBAC Contract

### Environment Matrix

| Component | Development (`dev` branch) | Production (`main` branch) |
|---|---|---|
| **Database** | `ECOMM_DEV` | `ECOMM_PROD` |
| **Schemas** | `RAW`, `MART`, `APP` | `RAW`, `MART`, `APP` |
| **ETL Warehouse** | `ECOMM_ETL_WH` (X-Small, 60s) | `ECOMM_ETL_WH` (X-Small, 60s) |
| **App Warehouse** | `ECOMM_APP_WH` (X-Small, 60s) | `ECOMM_APP_WH` (X-Small, 60s) |
| **GitHub Environment** | `dev` | `prod` (manual approval optional) |

### Role Hierarchy (Least Privilege)

| Role | Purpose | Key Grants |
|---|---|---|
| `ECOMM_CICD_ROLE` | Assigned to `SVC_GITHUB_CICD` (`TYPE = SERVICE`) | Owns schemas/tables/stages/Streamlit apps in `ECOMM_DEV` and `ECOMM_PROD`; `USAGE` on warehouses. |
| `ECOMM_APP_ROLE` | Runtime execution role for the Streamlit app | `USAGE` on `ECOMM_APP_WH`, `SELECT` on `MART.*` (including `FUTURE TABLES/VIEWS`), `USAGE` on `APP`. No write access to `RAW`. |
| `ECOMM_VIEWER_ROLE` | Business users / executives | `USAGE` on `ECOMM_APP_WH` and `APP` schema, `USAGE` on the Streamlit app object only. |

### Core Data Model

* **`RAW` Schema (Landing Layer):**
  * `RAW_CUSTOMERS` (`customer_id`, `signup_date`, `region`, `acquisition_channel`)
  * `RAW_PRODUCTS` (`sku_id`, `product_name`, `category`, `unit_cost`, `unit_price`, `stock_on_hand`, `reorder_point`)
  * `RAW_ORDERS` (`order_id`, `customer_id`, `order_ts`, `order_status`, `shipping_cost`, `discount_amount`)
  * `RAW_ORDER_ITEMS` (`order_item_id`, `order_id`, `sku_id`, `quantity`, `unit_price`)
* **`MART` Schema (Analytics Layer):**
  * `FACT_ORDERS` (Denormalized line-item + order grain; date-partitioned/clustered for scale benchmarks)
  * `DIM_CUSTOMERS` (Customer lifetime value, first order date, order count, segment)
  * `DIM_PRODUCTS` (SKU performance, 30-day sell-through velocity, stockout risk flag)
  * `AGG_DAILY_REVENUE` (Pre-aggregated daily revenue, orders, AOV, net margin by category/region)

---

## 4. Phased Implementation Roadmap

Tags: **`[HUMAN]`** = manual account/dashboard step | **`[AI]`** = code/config generation step

### Phase 0: Workspace & Account Setup (30 min)
* [ ] `[HUMAN]` Create a free 30-day Snowflake trial account and note your Account Identifier (`orgname-accountname`).
* [ ] `[HUMAN]` Initialize a public GitHub repository (`snowflake-ecomm-platform`) with `main` and `dev` branches.
* [ ] `[HUMAN]` Install Snowflake CLI (`pip install snowflake-cli`) and verify `snow --version`.
* [ ] `[AI]` Scaffold repository directory tree, `.gitignore`, `.env.example`, and `pyproject.toml` (with `ruff`, `pytest`, `streamlit`, `snowflake-snowpark-python`, `pandas`, `altair`).

**Definition of Done:** Repo cloned locally, `dev` branch active, `ruff check .` runs cleanly.

---

### Phase 1: Pillar 1 — Snowflake Basics, Governance & Ingestion
* [ ] `[AI]` Write `sql/01_bootstrap_account.sql` to create `ECOMM_ETL_WH`, `ECOMM_APP_WH` (X-Small, `AUTO_SUSPEND = 60`), `ECOMM_DEV`, `ECOMM_PROD`, RBAC roles, future grants, and service user `SVC_GITHUB_CICD` (`TYPE = SERVICE`).
* [ ] `[AI]` Write `sql/02_schemas_and_stages.sql` and `sql/03_raw_tables.sql` (parameterized by database name so the same script runs in `ECOMM_DEV` and `ECOMM_PROD`).
* [ ] `[AI]` Build `scripts/generate_data.py` to generate realistic synthetic e-commerce Parquet/CSV files with a `--rows` flag (`--rows 50000` for fast Dev loads, `--rows 5000000` for Prod/performance benchmarks).
* [ ] `[AI]` Write `sql/04_load_copy_into.sql` using internal stage `@RAW.ECOMM_STAGE` and `COPY INTO` with explicit `FILE_FORMAT` and `ON_ERROR = 'ABORT_STATEMENT'`.
* [ ] `[AI]` Build `scripts/run_snowpark_etl.py` using the Snowpark `Session` API to transform `RAW` tables into `MART` tables (`FACT_ORDERS`, `DIM_CUSTOMERS`, `DIM_PRODUCTS`, `AGG_DAILY_REVENUE`).
* [ ] `[HUMAN]` Execute `01_bootstrap_account.sql` once in Snowsight as `ACCOUNTADMIN`, upload generated dev data via `snow stage copy`, and run ingestion + Snowpark ETL against `ECOMM_DEV`.

**Definition of Done:** All 4 `RAW` tables and 4 `MART` tables are populated in `ECOMM_DEV`; `ECOMM_APP_ROLE` can `SELECT` from `MART` and gets permission denied on `RAW`.

---

### Phase 2: Pillar 2 — Streamlit in Snowflake (SiS) Application
* [ ] `[AI]` Implement `app/utils/snowflake_session.py`:
  * Attempt `snowflake.snowpark.context.get_active_session()` (runs natively inside SiS).
  * Fall back to `st.connection("snowflake").session()` for local development.
* [ ] `[AI]` Implement `app/utils/queries.py` using `@st.cache_data(ttl=600)` and bound parameters for all queries (read from `AGG_DAILY_REVENUE` for fast executive loads, `FACT_ORDERS` only on drill-down).
* [ ] `[AI]` Build the 4 modular views in `app/views/`:
  1. **Executive KPIs:** Date-range & category filters, Total Revenue, Orders, AOV, Net Margin, daily trend chart.
  2. **Cohort Retention:** Monthly signup cohort heatmap showing repeat purchase rates.
  3. **Inventory & SKU Health:** Low-stock alerts (`stock_on_hand <= reorder_point`), top/bottom margin SKUs.
  4. **Order Lookup:** Parameterized search by `order_id` or `customer_id` returning full line-item breakdown.
* [ ] `[AI]` Configure `snowflake.yml` for `snow streamlit deploy` (targeting Container Runtime with `pyproject.toml`) and create a matching `app/environment.yml` documenting Warehouse Runtime equivalents.
* [ ] `[AI]` Add unit tests in `tests/test_queries.py` and `tests/test_data_generator.py`.
* [ ] `[HUMAN]` Test locally (`streamlit run app/streamlit_app.py`), then deploy to `ECOMM_DEV.APP` via `snow streamlit deploy --replace`.

**Definition of Done:** App runs identically locally and inside Snowsight (`ECOMM_DEV.APP`); toggling filters hits `@st.cache_data` without re-querying Snowflake on repeated selections.

---

### Phase 3: Pillar 3 — CI/CD with GitHub Actions & Key-Pair Auth
* [ ] `[HUMAN]` Generate an encrypted RSA 2048-bit key pair locally (`openssl genrsa` + `openssl rsa -pubout`), strip PEM headers from the public key, and assign it via `ALTER USER SVC_GITHUB_CICD SET RSA_PUBLIC_KEY = '...'`.
* [ ] `[HUMAN]` Create GitHub Environments (`dev` and `prod`) and store secrets: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`, `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, `SNOWFLAKE_ROLE`.
* [ ] `[AI]` Write `.github/workflows/ci.yml`: runs `ruff check`, `ruff format --check`, and `pytest` on every Pull Request.
* [ ] `[AI]` Write `.github/workflows/deploy-dev.yml`: triggers on push to `dev`, configures `snow` CLI with key-pair auth, runs idempotent SQL migrations against `ECOMM_DEV`, and executes `snow streamlit deploy --replace --environment dev`.
* [ ] `[AI]` Write `.github/workflows/deploy-prod.yml`: triggers on push/merge to `main`, runs checks, applies SQL migrations to `ECOMM_PROD`, and deploys the SiS app to `ECOMM_PROD.APP`.
* [ ] `[HUMAN]` Push a test commit to `dev`, verify green Actions run + updated Dev app; open a PR to `main`, merge, and verify `ECOMM_PROD.APP` deploys automatically.

**Definition of Done:** Both `dev` and `prod` apps deploy hands-free via GitHub Actions with zero manual Snowsight copy-pasting and zero passwords stored.

---

### Phase 4: Pillar 4 — SQL Performance Tuning & Cost Proof
* [ ] `[HUMAN]` Generate a large dataset (`python scripts/generate_data.py --rows 5000000`), stage it, and load it into `ECOMM_DEV.RAW` -> `ECOMM_DEV.MART.FACT_ORDERS` in unsorted/random date order so Snowflake creates many overlapping micro-partitions.
* [ ] `[AI]` Write `sql/05_mart_and_clustering.sql` containing:
  * `ALTER SESSION SET USE_CACHED_RESULT = FALSE;` (ensures fair benchmark without 24h Result Cache masking compute).
  * Baseline query filtering a 7-day `ORDER_DATE` window + `PRODUCT_CATEGORY`.
  * Inspection queries (`SYSTEM$CLUSTERING_INFORMATION('FACT_ORDERS', '(ORDER_DATE, CATEGORY)')`).
  * Optimized table creation via `ORDER BY ORDER_DATE, CATEGORY` on load and `ALTER TABLE ... CLUSTER BY (ORDER_DATE, CATEGORY)`.
* [ ] `[HUMAN]` Run the baseline query in Snowsight, capture the **Query Profile** screenshot showing `Partitions scanned` vs `Partitions total` (full-table scan).
* [ ] `[HUMAN]` Apply clustering/sorted load, re-run the exact same query with Result Cache disabled, and capture the pruned **Query Profile** screenshot.
* [ ] `[HUMAN]` Re-enable `USE_CACHED_RESULT = TRUE`, re-run the query to verify instant metadata/result-cache serving, and save all metrics + screenshots into `docs/performance.md`.

**Definition of Done:** `docs/performance.md` contains concrete before/after tables (`Partitions scanned`, `Bytes scanned`, execution time) and an explanation of why auto-clustering is avoided on small dimension tables (`DIM_PRODUCTS`).

---

### Phase 5: Polish & Interview Readiness
* [ ] `[AI]` Write a crisp, recruiter-ready `README.md` with architecture diagram, local setup, CI/CD flow, runtime decision rationale (Container vs. Warehouse), and link to `docs/performance.md`.
* [ ] `[HUMAN]` Record a 2-minute silent GIF or Loom of the live SiS dashboard and add screenshots to `README.md`.

---

## 5. Technical Decision Notes (Interview Reference)

Keep these concise explanations handy when walking the recruiter or hiring manager through the repo:

* **Container Runtime vs. Warehouse Runtime:** Container Runtime (SPCS) uses `pyproject.toml` / PyPI packages, supports newer Streamlit versions, and handles concurrent viewers more efficiently than legacy Warehouse Runtime (`environment.yml` / Snowflake Anaconda channel). This repo supports Container Runtime primarily while documenting `environment.yml` for teams on legacy setups.
* **Why `TYPE = SERVICE` + Key-Pair Auth:** Snowflake enforces MFA on interactive human accounts. Automated GitHub Actions pipelines must use `TYPE = SERVICE` users authenticated via RSA key pairs (or OIDC) to stay secure and unbroken in CI.
* **Why Separate ETL and App Warehouses:** Heavy nightly `COPY INTO` or Snowpark batch jobs running on `ECOMM_ETL_WH` will never queue or slow down interactive executive queries running on `ECOMM_APP_WH`.
* **Why Clustering Isn't Free:** Background automatic reclustering consumes serverless credits. For tables under ~1 TB, sorting data during batch loads (`ORDER BY`) gives micro-partition pruning for free; explicit `CLUSTER BY` is reserved for multi-terabyte, continuously appended fact tables.
* **E-Commerce Migration Strategy:** Moving an e-commerce store off Postgres/MySQL into Snowflake works best via CDC or incremental batch extracts into `RAW` via `COPY INTO`, preserving immutable history in `RAW` and serving clean, aggregated `MART` tables to Streamlit apps.

---

## 6. How to Prompt Your AI Assistant With This Plan

When executing this project with an AI coding tool, work **one phase at a time** using prompts like:

* *"Read `plan.md`. Execute Phase 0 completely and verify all checks in its Definition of Done."*
* *"Read `plan.md`. Implement all `[AI]` tasks in Phase 1 following the exact naming conventions in Section 3."*
* *"Read `plan.md`. Implement Phase 2 (`app/` and `tests/`). Ensure every query uses `@st.cache_data` and bound parameters."*