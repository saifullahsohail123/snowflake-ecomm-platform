"""Main entry point for the E-Commerce Analytics & Operations Streamlit app."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure repo root is on sys.path both locally and inside Streamlit in Snowflake
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import streamlit as st  # noqa: E402

from app.utils.snowflake_session import get_snowflake_session, is_running_in_sis  # noqa: E402
from app.views import (  # noqa: E402
    cohort_retention,
    executive_kpi,
    inventory_alerts,
    order_lookup,
)

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
