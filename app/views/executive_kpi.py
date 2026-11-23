"""Executive KPIs view: revenue, orders, AOV, net margin, and trend charts."""

from __future__ import annotations

from datetime import timedelta

import altair as alt
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
