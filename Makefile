.PHONY: setup install docker-up docker-down docker-logs producer pull beam spark run-all clean

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

clean:
	find . -name "__pycache__" -not -path "./venv/*" -exec rm -rf {} +
