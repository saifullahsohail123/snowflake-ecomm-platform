"""Snowpark Session API ETL pipeline: transforms RAW landing tables into MART analytics tables."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from dotenv import load_dotenv
from snowflake.snowpark import Session, Window
from snowflake.snowpark import functions as F


def _load_private_key_bytes(
    key_path: str | None, key_pem: str | None, passphrase: str | None
) -> bytes:
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
        raise RuntimeError("Provide --connection <name> or SNOWFLAKE_PRIVATE_KEY(_PATH) env vars.")

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
        joined.with_column("ALLOCATED_DISCOUNT", F.round(F.col("DISCOUNT_AMOUNT") * alloc_ratio, 2))
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
    parser.add_argument(
        "--connection", default=None, help="Snowflake CLI connection name (e.g. dev)"
    )
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
