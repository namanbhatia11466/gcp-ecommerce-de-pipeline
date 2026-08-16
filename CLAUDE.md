# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

End-to-end GCP data engineering pipeline processing e-commerce orders:
`Pub/Sub → Apache Beam → GCS → PySpark → BigQuery`. Designed to run entirely
locally at $0 cost (DirectRunner + free-tier GCS/BigQuery), with code intended
to be Dataflow-ready for production later.

## Commands

```bash
# Setup
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt

# GCP auth: place a service account key at ./key.json (gitignored),
# and set GOOGLE_APPLICATION_CREDENTIALS in .env to point at it.
# Copy .env and fill in GCP_PROJECT_ID, GCP_BUCKET_NAME, BQ_RAW_DATASET, etc.

# Local infra (Airflow + Spark master via docker-compose)
docker compose up -d

# Run the pipeline stages in order
python ingestion/producer.py --num-events 20   # publish fake orders to Pub/Sub
python scripts/pull_messages.py                # pull from Pub/Sub sub -> GCS landing/
python pipeline/beam_pipeline.py               # validate/enrich -> GCS raw/ (DirectRunner)
python spark/transform.py                      # clean/transform -> load to BigQuery
```

There is no test suite yet (`pytest` is in requirements.txt but unused), and
`Makefile` is currently empty despite being tracked — don't assume `make`
targets exist until they're added.

## Architecture

Data flows through four independent, file-based stages — each stage reads
from GCS/Pub/Sub and writes to the next location, so stages can be re-run
independently and batches can be replayed:

1. **`ingestion/producer.py`** — generates fake orders (Faker) and publishes
   JSON to the `orders-raw-topic` Pub/Sub topic.
2. **`scripts/pull_messages.py`** — pulls from the `orders-sub` subscription,
   batches messages into a single JSONL file, writes to
   `gs://<bucket>/landing/orders/`, then acks.
3. **`pipeline/beam_pipeline.py`** — Apache Beam (DirectRunner) reads all
   landing JSONL, validates required fields (`order_id`, `user_id`,
   `product_id`, `amount`, `status`) via `ValidateAndEnrich`, and enriches
   valid records with `processed_at` / `value_tier`. Invalid records are
   routed to a **dead-letter** GCS path (`dead_letter/orders`) via
   `TaggedOutput` instead of failing the pipeline — this pattern should be
   preserved in any pipeline changes. Output goes to `gs://<bucket>/raw/orders/`.
4. **`spark/transform.py`** — PySpark reads raw JSONL from GCS, casts types,
   derives `order_date` (from `created_at`), `revenue` (`quantity * unit_price`),
   and `order_size` (bulk/standard/single), dedupes on `order_id`, drops nulls
   on critical fields, then writes to BigQuery
   (`{PROJECT_ID}.{BQ_RAW_DATASET}.orders`) partitioned by `order_date` and
   clustered by `status, product_id`. Uses the `spark-bigquery-with-dependencies`
   connector, auth'd via the same service account key.

All four scripts load config from `.env` via `python-dotenv` (never hardcode
project/bucket names). `docker-compose.yml` runs local Airflow
(webserver + scheduler + Postgres) and a Spark master, both mounted with the
same `key.json`, in preparation for orchestrating these stages as a DAG.

**Not yet implemented** (empty placeholder dirs — check before assuming
something exists): `airflow/dags/` (no DAG wires the 4 stages together yet),
`dbt_project/` (no staging/marts modeling on top of the raw BigQuery table),
`sql/ddl/`, `sql/analysis/`, `docs/`.

## Credentials

`key.json`, `.env`, and `venv/` are gitignored — never commit them or print
their contents. `.env` holds `GCP_PROJECT_ID`, `GCP_REGION`, `GCP_BUCKET_NAME`,
`BQ_RAW_DATASET`/`BQ_STAGING_DATASET`/`BQ_MARTS_DATASET`, Airflow admin
creds, and `GOOGLE_APPLICATION_CREDENTIALS`.

## Git workflow

Commit directly to `main` (no feature branches) after each logical chunk of
work. Do not push to `origin` — the user pushes manually, at a time of their
choosing (e.g. end of day).
