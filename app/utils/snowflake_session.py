"""Unified Snowflake session resolver for Streamlit in Snowflake (SiS) and local dev."""

from __future__ import annotations

import streamlit as st
from snowflake.snowpark import Session
from snowflake.snowpark.context import get_active_session


def is_running_in_sis() -> bool:
    """Return True when executing inside Streamlit in Snowflake."""
    try:
        get_active_session()
        return True
    except Exception:
        return False


def get_snowflake_session() -> Session:
    """Return active Snowpark Session inside SiS, or fall back to local st.connection."""
    try:
        return get_active_session()
    except Exception:
        conn = st.connection("snowflake")
        return conn.session()

