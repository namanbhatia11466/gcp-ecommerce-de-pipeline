import json
import time
import random
import argparse
import sys
from datetime import datetime
from faker import Faker
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import os

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

fake = Faker()
LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev" if LOCAL_MODE else None)
TOPIC_ID = "orders-raw-topic"

def generate_order():
    return {
        "order_id": fake.uuid4(),
        "user_id": random.randint(1000, 9999),
        "product_id": random.choice(["P001", "P002", "P003", "P004", "P005"]),
        "product_name": random.choice([
            "Laptop", "Phone", "Headphones", "Tablet", "Smartwatch"
        ]),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10.0, 1000.0), 2),
        "amount": round(random.uniform(10.0, 5000.0), 2),
        "currency": "USD",
        "status": random.choice(["placed", "paid", "shipped", "delivered"]),
        "country": fake.country(),
        "created_at": datetime.utcnow().isoformat()
    }

def publish_orders(num_events: int, delay: float):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    print(f"Publishing {num_events} orders to {topic_path}...")
    print("-" * 50)

    for i in range(num_events):
        order = generate_order()
        data = json.dumps(order).encode("utf-8")
        future = publisher.publish(topic_path, data)
        print(f"[{i+1}/{num_events}] Published order {order['order_id']} | "
              f"${order['amount']} | {order['status']}")
        future.result()  # wait for confirmation
        time.sleep(delay)

    print("-" * 50)
    print(f"✅ Done! {num_events} orders published to Pub/Sub.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    publish_orders(args.num_events, args.delay)