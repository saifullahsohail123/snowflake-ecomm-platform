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

