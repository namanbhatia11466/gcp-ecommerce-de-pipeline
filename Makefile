.PHONY: setup install docker-up docker-down docker-logs producer pull beam spark run-all \
        local-up local-down producer-local pull-local beam-local spark-local run-all-local clean

setup:
	python -m venv venv

install:
	pip install -r requirements.txt

docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

producer:
	python ingestion/producer.py --num-events 20

pull:
	python scripts/pull_messages.py

beam:
	python pipeline/beam_pipeline.py

spark:
	python spark/transform.py

run-all: producer pull beam spark

# ── LOCAL_MODE: zero-cost, no GCP account needed ────────────────────────
# Pub/Sub emulator + a local data/ directory standing in for GCS/BigQuery.
# See docker-compose.yml for why GCS/BigQuery aren't emulated the same way.

local-up:
	docker compose --profile local up -d pubsub-emulator
	PUBSUB_EMULATOR_HOST=localhost:8085 LOCAL_MODE=true python scripts/local_setup.py

local-down:
	docker compose --profile local down

producer-local:
	PUBSUB_EMULATOR_HOST=localhost:8085 LOCAL_MODE=true python ingestion/producer.py --num-events 20

pull-local:
	PUBSUB_EMULATOR_HOST=localhost:8085 LOCAL_MODE=true python scripts/pull_messages.py

beam-local:
	LOCAL_MODE=true python pipeline/beam_pipeline.py

spark-local:
	LOCAL_MODE=true python spark/transform.py

run-all-local: local-up producer-local pull-local beam-local spark-local

clean:
	find . -name "__pycache__" -not -path "./venv/*" -exec rm -rf {} +
