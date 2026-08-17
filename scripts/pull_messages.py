import json
import os
import sys
from google.cloud import pubsub_v1
from dotenv import load_dotenv
from datetime import datetime
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev" if LOCAL_MODE else None)
BUCKET = os.getenv("GCP_BUCKET_NAME")
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/orders-sub"
LOCAL_DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "gcs"

def pull_and_save(max_messages: int = 50):
    subscriber = pubsub_v1.SubscriberClient()

    if not LOCAL_MODE:
        from google.cloud import storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET)

    print(f"Pulling messages from {SUBSCRIPTION}...")

    response = subscriber.pull(
        request={
            "subscription": SUBSCRIPTION,
            "max_messages": max_messages
        }
    )

    if not response.received_messages:
        print("No messages found. Run producer.py first.")
        return

    # Collect all messages
    orders = []
    ack_ids = []
    for msg in response.received_messages:
        order = json.loads(msg.message.data.decode("utf-8"))
        orders.append(order)
        ack_ids.append(msg.ack_id)

    # Save as a single JSONL file - GCS in the cloud, a local directory
    # standing in for the bucket under LOCAL_MODE.
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"landing/orders/orders_{timestamp}.jsonl"
    content = "\n".join(json.dumps(o) for o in orders)

    if LOCAL_MODE:
        dest = LOCAL_DATA_ROOT / filename
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(content, encoding="utf-8")
        location = str(dest)
    else:
        blob = bucket.blob(filename)
        blob.upload_from_string(content, content_type="application/json")
        location = f"gs://{BUCKET}/{filename}"

    # Acknowledge messages
    subscriber.acknowledge(
        request={"subscription": SUBSCRIPTION, "ack_ids": ack_ids}
    )

    print(f"✅ Saved {len(orders)} orders to {location}")
    return filename

if __name__ == "__main__":
    pull_and_save(max_messages=50)