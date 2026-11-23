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

