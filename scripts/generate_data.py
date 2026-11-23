"""Synthetic e-commerce dataset generator (scalable from 50k to 5M+ line items)."""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd

REGIONS = ["North America", "Europe", "APAC", "LATAM"]
REGION_WEIGHTS = [0.45, 0.28, 0.18, 0.09]

CHANNELS = ["Organic", "Paid Search", "Social", "Email", "Referral"]
CHANNEL_WEIGHTS = [0.32, 0.26, 0.20, 0.14, 0.08]

CATEGORIES = [
    "Electronics",
    "Apparel",
    "Home & Kitchen",
    "Beauty",
    "Sports",
    "Accessories",
]

ADJECTIVES = [
    "Pro",
    "Ultra",
    "Compact",
    "Essential",
    "Classic",
    "Smart",
    "Eco",
    "Studio",
    "Travel",
    "Peak",
]

NOUNS_BY_CATEGORY = {
    "Electronics": ["Headphones", "Webcam", "Charger", "Speaker", "SSD", "Keyboard"],
    "Apparel": ["Hoodie", "Sneakers", "Jacket", "Tee", "Chinos", "Cap"],
    "Home & Kitchen": ["Blender", "Kettle", "Lamp", "Pan Set", "Air Purifier", "Mug"],
    "Beauty": ["Serum", "Cleanser", "Moisturizer", "Sunscreen", "Toner", "Mask"],
    "Sports": ["Yoga Mat", "Dumbbell", "Water Bottle", "Resistance Band", "Backpack"],
    "Accessories": ["Watch Band", "Phone Case", "Laptop Sleeve", "Sunglasses", "Wallet"],
}

ORDER_STATUSES = ["COMPLETED", "RETURNED", "CANCELLED"]
ORDER_STATUS_WEIGHTS = [0.88, 0.07, 0.05]


def generate_customers(
    num_customers: int,
    rng: np.random.Generator,
    start_date: str = "2024-01-01",
    end_date: str = "2025-12-31",
) -> pd.DataFrame:
    """Generate customer dimension rows with signup dates, regions, and channels."""
    start_ts = pd.Timestamp(start_date)
    total_days = (pd.Timestamp(end_date) - start_ts).days
    signup_offsets = rng.integers(0, max(total_days, 1), size=num_customers)
    signup_dates = (start_ts + pd.to_timedelta(signup_offsets, unit="D")).date

    return pd.DataFrame(
        {
            "customer_id": [f"CUST_{i:07d}" for i in range(1, num_customers + 1)],
            "signup_date": signup_dates,
            "region": rng.choice(REGIONS, size=num_customers, p=REGION_WEIGHTS),
            "acquisition_channel": rng.choice(CHANNELS, size=num_customers, p=CHANNEL_WEIGHTS),
        }
    )


def generate_products(num_products: int, rng: np.random.Generator) -> pd.DataFrame:
    """Generate SKU catalog where ~20% of SKUs intentionally sit at or below reorder point."""
    categories = rng.choice(CATEGORIES, size=num_products)
    names = [
        f"{rng.choice(ADJECTIVES)} {rng.choice(NOUNS_BY_CATEGORY[cat])} {i:03d}"
        for i, cat in enumerate(categories, start=1)
    ]

    unit_prices = np.round(rng.uniform(12.0, 420.0, size=num_products), 2)
    margin_ratios = rng.uniform(0.32, 0.72, size=num_products)
    unit_costs = np.round(unit_prices * (1.0 - margin_ratios), 2)

    reorder_points = rng.integers(15, 80, size=num_products)
    stock_on_hand = rng.integers(25, 400, size=num_products)

    # Flag ~20% as low-stock / stockout risk so Inventory Alerts view is immediately informative
    low_stock_mask = rng.random(size=num_products) < 0.20
    stock_on_hand[low_stock_mask] = rng.integers(0, reorder_points[low_stock_mask] + 1)

    return pd.DataFrame(
        {
            "sku_id": [f"SKU_{i:05d}" for i in range(1, num_products + 1)],
            "product_name": names,
            "category": categories,
            "unit_cost": unit_costs,
            "unit_price": unit_prices,
            "stock_on_hand": stock_on_hand.astype(np.int64),
            "reorder_point": reorder_points.astype(np.int64),
        }
    )


def generate_orders_chunk(
    chunk_idx: int,
    item_start_id: int,
    order_start_id: int,
    num_items: int,
    customers_df: pd.DataFrame,
    products_df: pd.DataFrame,
    rng: np.random.Generator,
    max_order_date: date,
    unsorted: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Generate one chunk of referentially consistent orders and order line items."""
    # Average ~2 items per order
    num_orders = max(1, int(np.ceil(num_items / 2.0)))

    cust_indices = rng.integers(0, len(customers_df), size=num_orders)
    chosen_cust_ids = customers_df["customer_id"].to_numpy()[cust_indices]
    cust_signups = pd.to_datetime(customers_df["signup_date"].to_numpy()[cust_indices])

    max_ts = pd.Timestamp(max_order_date)
    days_since_signup = np.maximum(1, (max_ts - cust_signups).days.to_numpy())
    # Bias offsets slightly toward early repeat purchases for realistic cohort retention decay
    order_day_offsets = np.floor(rng.beta(0.85, 1.4, size=num_orders) * days_since_signup).astype(
        int
    )
    seconds_in_day = rng.integers(0, 86400, size=num_orders)
    order_timestamps = (
        cust_signups
        + pd.to_timedelta(order_day_offsets, unit="D")
        + pd.to_timedelta(seconds_in_day, unit="s")
    )

    order_ids = np.array([f"ORD_{order_start_id + i:09d}" for i in range(num_orders)])
    order_statuses = rng.choice(ORDER_STATUSES, size=num_orders, p=ORDER_STATUS_WEIGHTS)
    shipping_costs = np.round(rng.choice([0.0, 4.99, 7.99, 12.99], size=num_orders, p=[0.4, 0.3, 0.2, 0.1]), 2)
    discount_amounts = np.round(
        np.where(rng.random(num_orders) < 0.25, rng.uniform(5.0, 35.0, size=num_orders), 0.0), 2
    )

    orders_df = pd.DataFrame(
        {
            "order_id": order_ids,
            "customer_id": chosen_cust_ids,
            "order_ts": order_timestamps.astype("datetime64[us]"),
            "order_status": order_statuses,
            "shipping_cost": shipping_costs,
            "discount_amount": discount_amounts,
        }
    )

    if unsorted:
        orders_df = orders_df.sample(frac=1.0, random_state=int(rng.integers(0, 1_000_000))).reset_index(
            drop=True
        )
    else:
        orders_df = orders_df.sort_values("order_ts").reset_index(drop=True)

    # Ensure every order gets at least 1 item, plus random extra items up to num_items
    if num_items >= num_orders:
        base_order_idx = np.arange(num_orders)
        extra_order_idx = rng.integers(0, num_orders, size=num_items - num_orders)
        item_order_idx = np.concatenate([base_order_idx, extra_order_idx])
    else:
        item_order_idx = rng.integers(0, num_orders, size=num_items)

    rng.shuffle(item_order_idx)
    sku_indices = rng.integers(0, len(products_df), size=num_items)

    order_items_df = pd.DataFrame(
        {
            "order_item_id": [f"ITEM_{item_start_id + i:010d}" for i in range(num_items)],
            "order_id": order_ids[item_order_idx],
            "sku_id": products_df["sku_id"].to_numpy()[sku_indices],
            "quantity": rng.integers(1, 5, size=num_items).astype(np.int64),
            "unit_price": products_df["unit_price"].to_numpy()[sku_indices],
        }
    )

    return orders_df, order_items_df, num_orders


def _write_df(df: pd.DataFrame, path: Path, fmt: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        df.to_parquet(path, index=False, compression="snappy")
    elif fmt == "csv":
        df.to_csv(path, index=False)
    else:
        raise ValueError(f"Unsupported format: {fmt}")


def generate_dataset(
    rows: int,
    output_dir: Path,
    fmt: str = "parquet",
    seed: int = 42,
    unsorted: bool = False,
    chunk_size: int = 500_000,
) -> dict[str, int]:
    """Generate full synthetic e-commerce dataset into output_dir/{customers,products,orders,order_items}."""
    if rows <= 0:
        raise ValueError("--rows must be a positive integer.")

    rng = np.random.default_rng(seed)
    num_customers = min(max(1_000, rows // 10), 500_000)
    num_products = 250

    customers_df = generate_customers(num_customers=num_customers, rng=rng)
    products_df = generate_products(num_products=num_products, rng=rng)

    ext = "parquet" if fmt == "parquet" else "csv"
    _write_df(customers_df, output_dir / "customers" / f"customers.{ext}", fmt)
    _write_df(products_df, output_dir / "products" / f"products.{ext}", fmt)

    max_order_date = date(2026, 6, 30)
    remaining = rows
    chunk_idx = 0
    item_start = 1
    order_start = 1
    total_orders = 0

    while remaining > 0:
        cur_items = min(chunk_size, remaining)
        orders_df, items_df, chunk_orders = generate_orders_chunk(
            chunk_idx=chunk_idx,
            item_start_id=item_start,
            order_start_id=order_start,
            num_items=cur_items,
            customers_df=customers_df,
            products_df=products_df,
            rng=rng,
            max_order_date=max_order_date,
            unsorted=unsorted,
        )
        _write_df(orders_df, output_dir / "orders" / f"orders_part_{chunk_idx:04d}.{ext}", fmt)
        _write_df(
            items_df,
            output_dir / "order_items" / f"order_items_part_{chunk_idx:04d}.{ext}",
            fmt,
        )

        remaining -= cur_items
        item_start += cur_items
        order_start += chunk_orders
        total_orders += chunk_orders
        chunk_idx += 1

    return {
        "customers": num_customers,
        "products": num_products,
        "orders": total_orders,
        "order_items": rows,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic e-commerce data for Snowflake.")
    parser.add_argument("--rows", type=int, default=50_000, help="Number of order line items.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"), help="Output directory.")
    parser.add_argument("--format", choices=["parquet", "csv"], default="parquet")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--unsorted",
        action="store_true",
        help="Shuffle order timestamps across chunks (used in Phase 4 clustering benchmark).",
    )
    parser.add_argument("--chunk-size", type=int, default=500_000)
    args = parser.parse_args()

    started = datetime.now()
    # Clean previous files inside output_dir subfolders if re-generating
    for sub in ("customers", "products", "orders", "order_items"):
        sub_dir = args.output_dir / sub
        if sub_dir.exists():
            for f in sub_dir.iterdir():
                if f.is_file():
                    f.unlink()

    counts = generate_dataset(
        rows=args.rows,
        output_dir=args.output_dir,
        fmt=args.format,
        seed=args.seed,
        unsorted=args.unsorted,
        chunk_size=args.chunk_size,
    )
    elapsed = (datetime.now() - started).total_seconds()
    print(f"Generated dataset in {elapsed:.2f}s -> {counts} at {args.output_dir}")


if __name__ == "__main__":
    main()