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

