import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import json
import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_BUCKET_NAME")
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

        required = ["order_id", "user_id", "product_id", "amount", "status"]
        missing = [f for f in required if not order.get(f)]

        if missing or order.get("amount", 0) <= 0:
            yield beam.pvalue.TaggedOutput("dead_letter", json.dumps(order))
            return

        # Enrich
        order["processed_at"] = datetime.utcnow().isoformat()
        order["status"] = order.get("status", "").upper()
        amount = order.get("amount", 0)
        order["value_tier"] = (
            "high" if amount >= 1000 else
            "medium" if amount >= 100 else
            "low"
        )

        yield json.dumps(order)


def run():
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
            | "ReadFromGCS"     >> beam.io.ReadFromText(INPUT_PATH)
            | "ValidateEnrich"  >> beam.ParDo(ValidateAndEnrich()).with_outputs(
                                        "dead_letter", main="valid"
                                   )
        )

        # Write valid orders
        (
            results.valid
            | "WriteValid" >> beam.io.WriteToText(
                OUTPUT_PATH,
                file_name_suffix=".jsonl",
                shard_name_template="-SS-of-NN"
              )
        )

        # Write dead letter
        (
            results.dead_letter
            | "WriteDeadLetter" >> beam.io.WriteToText(
                DEAD_LETTER_PATH,
                file_name_suffix=".jsonl",
                shard_name_template="-SS-of-NN"
              )
        )

    print("-" * 50)
    print("✅ Pipeline complete!")
    print(f"   Check output: gsutil ls gs://{BUCKET}/raw/orders/")


if __name__ == "__main__":
    run()