"""Unit tests for parameterized SQL query builders and KPI metric calculations."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pandas as pd
import pytest

from app.utils.queries import (
    build_in_placeholders,
    compute_kpi_metrics,
    compute_mom_growth,
    get_daily_revenue,
    get_inventory_health,
    lookup_orders,
)


def _mock_session_with_df(df: pd.DataFrame) -> MagicMock:
    session = MagicMock()
    session.sql.return_value.to_pandas.return_value = df
    return session


def test_build_in_placeholders() -> None:
    assert build_in_placeholders(1) == "?"
    assert build_in_placeholders(3) == "?, ?, ?"
    with pytest.raises(ValueError):
        build_in_placeholders(0)


def test_compute_kpi_metrics_uses_weighted_aov() -> None:
    df = pd.DataFrame(
        {
            "NET_REVENUE": [1000.0, 200.0],
            "ORDER_COUNT": [10, 1],
            "NET_MARGIN": [400.0, 80.0],
        }
    )
    metrics = compute_kpi_metrics(df)
    assert metrics["total_revenue"] == 1200.0
    assert metrics["total_orders"] == 11.0
    assert metrics["aov"] == round(1200.0 / 11.0, 2)
    assert metrics["net_margin"] == 480.0
    assert metrics["margin_pct"] == 40.0


def test_compute_kpi_metrics_empty_df() -> None:
    metrics = compute_kpi_metrics(pd.DataFrame())
    assert metrics == {
        "total_revenue": 0.0,
        "total_orders": 0.0,
        "aov": 0.0,
        "net_margin": 0.0,
        "margin_pct": 0.0,
    }


def test_compute_mom_growth() -> None:
    assert compute_mom_growth(125.0, 100.0) == 25.0
    assert compute_mom_growth(100.0, 0.0) is None


def test_get_daily_revenue_uses_bound_parameters() -> None:
    sample = pd.DataFrame(
        {
            "ORDER_DATE": ["2026-01-01"],
            "CATEGORY": ["Electronics"],
            "REGION": ["North America"],
            "ORDER_COUNT": [5],
            "UNIQUE_CUSTOMERS": [5],
            "UNITS_SOLD": [8],
            "GROSS_REVENUE": [500.0],
            "NET_REVENUE": [480.0],
            "TOTAL_COGS": [200.0],
            "NET_MARGIN": [280.0],
        }
    )
    mock_session = _mock_session_with_df(sample)
    get_daily_revenue.clear()

    res = get_daily_revenue(
        mock_session,
        start_date=date(2026, 1, 1),
        end_date=date(2026, 1, 31),
        categories=("Electronics", "Apparel"),
        regions=("North America",),
    )

    assert len(res) == 1
    called_sql, called_kwargs = mock_session.sql.call_args
    sql_text = called_sql[0]
    params = called_kwargs["params"]

    assert "Electronics" not in sql_text
    assert "North America" not in sql_text
    assert "?, ?" in sql_text
    assert params == ["2026-01-01", "2026-01-31", "Electronics", "Apparel", "North America"]


def test_lookup_orders_prevents_sql_injection() -> None:
    mock_session = _mock_session_with_df(pd.DataFrame())
    lookup_orders.clear()

    malicious_input = "ORD_1' OR 1=1 --"
    lookup_orders(mock_session, search_type="Order ID", search_value=malicious_input, limit=50)

    called_sql, called_kwargs = mock_session.sql.call_args
    sql_text = called_sql[0]
    params = called_kwargs["params"]

    assert malicious_input not in sql_text
    assert "WHERE ORDER_ID = ?" in sql_text
    assert params == [malicious_input, 50]


def test_get_inventory_health_only_at_risk_clause() -> None:
    mock_session = _mock_session_with_df(pd.DataFrame())
    get_inventory_health.clear()

    get_inventory_health(mock_session, categories=("Beauty",), only_at_risk=True)
    called_sql, called_kwargs = mock_session.sql.call_args
    assert "STOCKOUT_RISK_FLAG = TRUE" in called_sql[0]
    assert called_kwargs["params"] == ["Beauty"]

