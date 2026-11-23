"""Unit tests for synthetic e-commerce dataset generation."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from scripts.generate_data import generate_dataset


def test_generate_dataset_parquet_integrity(tmp_path: Path) -> None:
    counts = generate_dataset(rows=250, output_dir=tmp_path, fmt="parquet", seed=123)

    assert counts["order_items"] == 250
    assert counts["products"] == 250
    assert counts["customers"] == 1_000

    customers = pd.read_parquet(tmp_path / "customers" / "customers.parquet")
    products = pd.read_parquet(tmp_path / "products" / "products.parquet")
    orders = pd.read_parquet(tmp_path / "orders" / "orders_part_0000.parquet")
    items = pd.read_parquet(tmp_path / "order_items" / "order_items_part_0000.parquet")

    assert len(items) == 250
    assert customers["customer_id"].is_unique
    assert products["sku_id"].is_unique
    assert orders["order_id"].is_unique
    assert items["order_item_id"].is_unique

    # Referential integrity checks
    assert set(orders["customer_id"]).issubset(set(customers["customer_id"]))
    assert set(items["order_id"]).issubset(set(orders["order_id"]))
    assert set(items["sku_id"]).issubset(set(products["sku_id"]))

    # Business constraints
    assert (products["unit_price"] > products["unit_cost"]).all()
    assert (products["stock_on_hand"] <= products["reorder_point"]).any()
    assert (items["quantity"] >= 1).all()


def test_generate_dataset_rejects_non_positive_rows(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="positive"):
        generate_dataset(rows=0, output_dir=tmp_path)

