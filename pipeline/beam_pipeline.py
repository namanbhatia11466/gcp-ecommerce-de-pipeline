import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from dotenv import load_dotenv

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev" if LOCAL_MODE else None)
BUCKET = os.getenv("GCP_BUCKET_NAME")

if LOCAL_MODE:
    # Local directory standing in for the bucket - see docker-compose.yml
    # for why GCS isn't emulated the same way Pub/Sub is.
    _DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "gcs"
    INPUT_PATH = str(_DATA_ROOT / "landing" / "orders" / "*.jsonl")
    OUTPUT_PATH = str(_DATA_ROOT / "raw" / "orders" / "orders")
    DEAD_LETTER_PATH = str(_DATA_ROOT / "dead_letter" / "orders")
else:
    INPUT_PATH = f"gs://{BUCKET}/landing/orders/*.jsonl"
    OUTPUT_PATH = f"gs://{BUCKET}/raw/orders/orders"
    DEAD_LETTER_PATH = f"gs://{BUCKET}/dead_letter/orders"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ValidateAndEnrich(beam.DoFn):
    """Validate and enrich each order. Invalid → dead letter."""

    def process(self, element):
        try:
            order = json.loads(element)
        except Exception:
            yield beam.pvalue.TaggedOutput("dead_letter", element)
            return

        required = ["order_id", "customer_id", "product_id", "amount", "status"]
        missing = [f for f in required if not order.get(f)]

        if missing or order.get("amount", 0) <= 0:
            yield beam.pvalue.TaggedOutput("dead_letter", json.dumps(order))
            return

        # Enrich
        order["processed_at"] = datetime.utcnow().isoformat()
        order["status"] = order.get("status", "").upper()
        amount = order.get("amount", 0)
        # Thresholds calibrated to the real Olist order-line amount
        # distribution (BRL): median ~81, 90th percentile ~255.
        order["value_tier"] = "high" if amount >= 250 else "medium" if amount >= 80 else "low"

        yield json.dumps(order)


def run():
    if LOCAL_MODE:
        options = PipelineOptions(runner="DirectRunner", project=PROJECT_ID)
        # WriteToText won't create parent directories itself.
        Path(OUTPUT_PATH).parent.mkdir(parents=True, exist_ok=True)
        Path(DEAD_LETTER_PATH).parent.mkdir(parents=True, exist_ok=True)
    else:
        options = PipelineOptions(
            runner="DirectRunner",
            project=PROJECT_ID,
            temp_location=f"gs://{BUCKET}/temp",
        )

    print("🚀 Starting Beam pipeline (batch mode)...")
    print(f"   Input:  {INPUT_PATH}")
    print(f"   Output: {OUTPUT_PATH}")
    print("-" * 50)

    with beam.Pipeline(options=options) as p:
        results = (
            p
            | "ReadFromGCS" >> beam.io.ReadFromText(INPUT_PATH)
            | "ValidateEnrich"
            >> beam.ParDo(ValidateAndEnrich()).with_outputs("dead_letter", main="valid")
        )

        # Write valid orders
        (
            results.valid
            | "WriteValid"
            >> beam.io.WriteToText(
                OUTPUT_PATH, file_name_suffix=".jsonl", shard_name_template="-SS-of-NN"
            )
        )

        # Write dead letter
        (
            results.dead_letter
            | "WriteDeadLetter"
            >> beam.io.WriteToText(
                DEAD_LETTER_PATH, file_name_suffix=".jsonl", shard_name_template="-SS-of-NN"
            )
        )

    print("-" * 50)
    print("✅ Pipeline complete!")
    if LOCAL_MODE:
        print(f"   Check output: ls {Path(OUTPUT_PATH).parent}")
    else:
        print(f"   Check output: gsutil ls gs://{BUCKET}/raw/orders/")


if __name__ == "__main__":
    run()
