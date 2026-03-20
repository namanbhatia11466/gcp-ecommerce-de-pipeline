import json
import os
from google.cloud import pubsub_v1, storage
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_BUCKET_NAME")
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/orders-sub"

def pull_and_save(max_messages: int = 50):
    subscriber = pubsub_v1.SubscriberClient()
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

    # Save to GCS as a single JSONL file
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"landing/orders/orders_{timestamp}.jsonl"
    content = "\n".join(json.dumps(o) for o in orders)

    blob = bucket.blob(filename)
    blob.upload_from_string(content, content_type="application/json")

    # Acknowledge messages
    subscriber.acknowledge(
        request={"subscription": SUBSCRIPTION, "ack_ids": ack_ids}
    )

    print(f"✅ Saved {len(orders)} orders to gs://{BUCKET}/{filename}")
    return filename

if __name__ == "__main__":
    pull_and_save(max_messages=50)