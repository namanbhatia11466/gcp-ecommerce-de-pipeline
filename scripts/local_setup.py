"""
One-time setup for LOCAL_MODE: creates the Pub/Sub topic + subscription
against the emulator (it starts empty, unlike a real GCP project where
these are provisioned once via Terraform/gcloud) and the local directory
standing in for the GCS bucket. Idempotent - safe to re-run.
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import AlreadyExists
from google.cloud import pubsub_v1

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev")
TOPIC_ID = "orders-raw-topic"
SUBSCRIPTION_ID = "orders-sub"
DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "gcs"


def main():
    if not os.getenv("PUBSUB_EMULATOR_HOST"):
        raise SystemExit(
            "PUBSUB_EMULATOR_HOST is not set - start the emulator first:\n"
            "  docker compose --profile local up -d pubsub-emulator\n"
            "  export PUBSUB_EMULATOR_HOST=localhost:8085   "
            "(Windows: set PUBSUB_EMULATOR_HOST=localhost:8085)"
        )

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    try:
        publisher.create_topic(request={"name": topic_path})
        print(f"✅ Created topic {topic_path}")
    except AlreadyExists:
        print(f"   Topic {topic_path} already exists")

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    try:
        subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
        print(f"✅ Created subscription {sub_path}")
    except AlreadyExists:
        print(f"   Subscription {sub_path} already exists")

    for sub_dir in ("landing/orders", "raw/orders", "dead_letter/orders"):
        (DATA_ROOT / sub_dir).mkdir(parents=True, exist_ok=True)
    print(f"✅ Local data directory ready at {DATA_ROOT}")


if __name__ == "__main__":
    main()
