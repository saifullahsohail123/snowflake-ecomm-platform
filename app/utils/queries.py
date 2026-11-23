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

