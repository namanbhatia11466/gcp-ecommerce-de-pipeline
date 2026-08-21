import pandas as pd

import ingestion.producer as producer
from ingestion.producer import load_orders, row_to_order


def test_row_to_order_maps_fields_correctly():
    row = pd.Series(
        {
            "order_id": "o1",
            "customer_id": "c1",
            "product_id": "p1",
            "product_category": "electronics",
            "quantity": 2,
            "unit_price": 10.5,
            "amount": 21.0,
            "freight_value": 5.0,
            "order_status": "delivered",
            "payment_type": "credit_card",
            "customer_state": "SP",
            "order_purchase_timestamp": "2018-01-01 12:00:00",
        }
    )
    order = row_to_order(row)

    assert order["order_id"] == "o1"
    assert order["status"] == "delivered"
    assert order["currency"] == "BRL"
    assert order["quantity"] == 2
    assert isinstance(order["quantity"], int)
    assert order["amount"] == 21.0


def _write_fixture_dataset(tmp_path):
    (tmp_path / "olist_orders_dataset.csv").write_text(
        "order_id,customer_id,order_status,order_purchase_timestamp\n"
        "o1,c1,delivered,2018-01-01 10:00:00\n"
    )
    (tmp_path / "olist_order_items_dataset.csv").write_text(
        "order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,freight_value\n"
        "o1,1,p1,s1,2018-01-01,10.00,2.00\n"
        "o1,2,p1,s1,2018-01-01,10.00,2.00\n"
        "o1,3,p2,s1,2018-01-01,30.00,3.00\n"
    )
    (tmp_path / "olist_order_payments_dataset.csv").write_text(
        "order_id,payment_sequential,payment_type,payment_installments,payment_value\n"
        "o1,1,credit_card,1,55.00\n"
    )
    (tmp_path / "olist_products_dataset.csv").write_text(
        "product_id,product_category_name\np1,eletronicos\np2,beleza_saude\n"
    )
    (tmp_path / "olist_customers_dataset.csv").write_text("customer_id,customer_state\nc1,SP\n")
    (tmp_path / "product_category_name_translation.csv").write_text(
        "product_category_name,product_category_name_english\n"
        "eletronicos,electronics\nbeleza_saude,health_beauty\n"
    )
    return tmp_path


def test_load_orders_collapses_repeated_items_into_quantity(tmp_path, monkeypatch):
    # This is the exact logic that once fed a real bug downstream: Spark
    # deduped on order_id alone, silently dropping legitimate order lines,
    # because this collapsing step (not order_id) is what actually defines
    # the (order_id, product_id) grain everything else depends on.
    fixture_dir = _write_fixture_dataset(tmp_path)
    monkeypatch.setattr(producer, "FULL_DIR", fixture_dir)

    df = load_orders()

    assert len(df) == 2  # one row per (order_id, product_id)

    p1_line = df[df["product_id"] == "p1"].iloc[0]
    assert p1_line["quantity"] == 2
    assert p1_line["unit_price"] == 10.00
    assert p1_line["amount"] == 20.00  # 2 * 10.00
    assert p1_line["freight_value"] == 4.00  # 2.00 + 2.00
    assert p1_line["product_category"] == "electronics"
    assert p1_line["customer_state"] == "SP"

    p2_line = df[df["product_id"] == "p2"].iloc[0]
    assert p2_line["quantity"] == 1
    assert p2_line["product_category"] == "health_beauty"
