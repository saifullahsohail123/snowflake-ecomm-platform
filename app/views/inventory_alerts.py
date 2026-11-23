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
        selected_cats = st.multiselect(
            "Filter Categories", options=all_categories, default=all_categories
        )
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
    k3.metric(
        "Average Unit Margin", f"{avg_margin:.1f}%", help=f"Total On-Hand Units: {total_units:,}"
    )

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
                tooltip=[
                    "SKU_ID",
                    "PRODUCT_NAME",
                    alt.Tooltip("TOTAL_NET_MARGIN:Q", format="$,.2f"),
                ],
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
