"""
One-time (re-runnable) script that carves a small, referentially-consistent
sample out of the full Olist dataset (data/raw/olist/) and writes it to
data/sample/olist/ - the same 6 files, just fewer rows. That sample is
committed to the repo so `producer.py` has real data to stream even before
anyone downloads the full ~99k-order dataset from Kaggle.

Run after downloading the full dataset:
    kaggle datasets download -d olistbr/brazilian-ecommerce -p data/raw/olist --unzip
    python scripts/build_local_sample.py
"""

import sys
from pathlib import Path

import pandas as pd

sys.stdout.reconfigure(encoding="utf-8")

FULL_DIR = Path(__file__).resolve().parent.parent / "data" / "raw" / "olist"
SAMPLE_DIR = Path(__file__).resolve().parent.parent / "data" / "sample" / "olist"
SAMPLE_ORDERS = 1000
SEED = 42


def main():
    orders = pd.read_csv(FULL_DIR / "olist_orders_dataset.csv")
    items = pd.read_csv(FULL_DIR / "olist_order_items_dataset.csv")
    payments = pd.read_csv(FULL_DIR / "olist_order_payments_dataset.csv")
    products = pd.read_csv(FULL_DIR / "olist_products_dataset.csv")
    customers = pd.read_csv(FULL_DIR / "olist_customers_dataset.csv")
    categories = pd.read_csv(FULL_DIR / "product_category_name_translation.csv")

    sampled_orders = orders.sample(n=SAMPLE_ORDERS, random_state=SEED)
    order_ids = set(sampled_orders["order_id"])

    sampled_items = items[items["order_id"].isin(order_ids)]
    sampled_payments = payments[payments["order_id"].isin(order_ids)]

    customer_ids = set(sampled_orders["customer_id"])
    sampled_customers = customers[customers["customer_id"].isin(customer_ids)]

    product_ids = set(sampled_items["product_id"])
    sampled_products = products[products["product_id"].isin(product_ids)]

    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    sampled_orders.to_csv(SAMPLE_DIR / "olist_orders_dataset.csv", index=False)
    sampled_items.to_csv(SAMPLE_DIR / "olist_order_items_dataset.csv", index=False)
    sampled_payments.to_csv(SAMPLE_DIR / "olist_order_payments_dataset.csv", index=False)
    sampled_customers.to_csv(SAMPLE_DIR / "olist_customers_dataset.csv", index=False)
    sampled_products.to_csv(SAMPLE_DIR / "olist_products_dataset.csv", index=False)
    categories.to_csv(SAMPLE_DIR / "product_category_name_translation.csv", index=False)

    print(
        f"✅ Wrote {SAMPLE_ORDERS} real orders ({len(sampled_items)} order lines) to {SAMPLE_DIR}"
    )


if __name__ == "__main__":
    main()
