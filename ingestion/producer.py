import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import pubsub_v1

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev" if LOCAL_MODE else None)
TOPIC_ID = "orders-raw-topic"

DATA_ROOT = Path(__file__).resolve().parent.parent / "data"
FULL_DIR = DATA_ROOT / "raw" / "olist"
SAMPLE_DIR = DATA_ROOT / "sample" / "olist"


def _dataset_dir():
    """Prefer the full ~99k-order Kaggle download; fall back to the small
    committed sample so the pipeline runs out of the box with no setup."""
    if (FULL_DIR / "olist_orders_dataset.csv").exists():
        return FULL_DIR, "full dataset (~99k orders)"
    if (SAMPLE_DIR / "olist_orders_dataset.csv").exists():
        return SAMPLE_DIR, "bundled sample (1,000 orders)"
    raise SystemExit(
        "No Olist dataset found under data/raw/olist or data/sample/olist.\n"
        "Download the full dataset with:\n"
        "  kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/olist --unzip"
    )


def load_orders() -> pd.DataFrame:
    """Real Olist orders, joined and collapsed to one row per (order,
    product) line - Olist has no 'quantity' field itself, each unit of a
    product is its own order_items row, so repeated rows for the same
    (order_id, product_id) become the quantity here."""
    data_dir, source_label = _dataset_dir()

    orders = pd.read_csv(data_dir / "olist_orders_dataset.csv")
    items = pd.read_csv(data_dir / "olist_order_items_dataset.csv")
    payments = pd.read_csv(data_dir / "olist_order_payments_dataset.csv")
    products = pd.read_csv(data_dir / "olist_products_dataset.csv")
    customers = pd.read_csv(data_dir / "olist_customers_dataset.csv")
    categories = pd.read_csv(data_dir / "product_category_name_translation.csv")

    lines = (
        items.groupby(["order_id", "product_id"])
        .agg(
            quantity=("order_item_id", "count"),
            unit_price=("price", "mean"),
            freight_value=("freight_value", "sum"),
        )
        .reset_index()
    )
    lines["unit_price"] = lines["unit_price"].round(2)
    lines["amount"] = (lines["quantity"] * lines["unit_price"]).round(2)
    lines["freight_value"] = lines["freight_value"].round(2)

    primary_payment = (
        payments.sort_values("payment_sequential")
        .groupby("order_id")["payment_type"]
        .first()
        .reset_index()
    )

    products = products.merge(categories, on="product_category_name", how="left")
    products["product_category"] = (
        products["product_category_name_english"]
        .fillna(products["product_category_name"])
        .fillna("unknown")
    )

    df = (
        lines.merge(
            orders[["order_id", "customer_id", "order_status", "order_purchase_timestamp"]],
            on="order_id",
            how="inner",
        )
        .merge(customers[["customer_id", "customer_state"]], on="customer_id", how="left")
        .merge(products[["product_id", "product_category"]], on="product_id", how="left")
        .merge(primary_payment, on="order_id", how="left")
    )

    df["product_category"] = df["product_category"].fillna("unknown")
    df["payment_type"] = df["payment_type"].fillna("not_defined")
    df["customer_state"] = df["customer_state"].fillna("NA")

    print(f"Loaded {len(df)} real order lines from {source_label} ({data_dir})")
    return df


def row_to_order(row: pd.Series) -> dict:
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "product_id": row["product_id"],
        "product_category": row["product_category"],
        "quantity": int(row["quantity"]),
        "unit_price": float(row["unit_price"]),
        "amount": float(row["amount"]),
        "freight_value": float(row["freight_value"]),
        "currency": "BRL",
        "status": row["order_status"],
        "payment_type": row["payment_type"],
        "customer_state": row["customer_state"],
        "created_at": row["order_purchase_timestamp"],
    }


def publish_orders(num_events: int, delay: float, seed: int = None):
    df = load_orders()
    sample = df.sample(n=min(num_events, len(df)), random_state=seed)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing {len(sample)} real orders to {topic_path}...")
    print("-" * 50)

    for i, (_, row) in enumerate(sample.iterrows()):
        order = row_to_order(row)
        data = json.dumps(order).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(
            f"[{i + 1}/{len(sample)}] Published order {order['order_id']} | "
            f"R${order['amount']} | {order['status']}"
        )
        future.result()  # wait for confirmation
        time.sleep(delay)

    print("-" * 50)
    print(f"✅ Done! {len(sample)} real orders published to Pub/Sub.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument(
        "--seed", type=int, default=None, help="Random seed for reproducible sampling"
    )
    args = parser.parse_args()

    publish_orders(args.num_events, args.delay, args.seed)
