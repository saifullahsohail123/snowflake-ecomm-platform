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

CREATE OR REPLACE FILE FORMAT RAW.PARQUET_FF
    TYPE = PARQUET
    COMPRESSION = SNAPPY
    USE_LOGICAL_TYPE = TRUE;

CREATE FILE FORMAT IF NOT EXISTS RAW.CSV_FF
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    NULL_IF = ('', 'NULL', 'null');

CREATE STAGE IF NOT EXISTS RAW.ECOMM_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal named stage for raw e-commerce Parquet/CSV ingestion';