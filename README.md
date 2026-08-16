# GCP E-Commerce Data Engineering Pipeline

End-to-end data pipeline built on Google Cloud Platform processing 
real-time e-commerce orders from ingestion to analytics.

## Architecture
```
Pub/Sub → Apache Beam → GCS → PySpark → BigQuery
```

## Pipeline Stages

| Stage | File | Description |
|---|---|---|
| Ingestion | `ingestion/producer.py` | Generates fake orders, publishes to Pub/Sub |
| Landing | `scripts/pull_messages.py` | Pulls messages from Pub/Sub, saves to GCS |
| Processing | `pipeline/beam_pipeline.py` | Validates and enriches orders via Apache Beam |
| Transform | `spark/transform.py` | PySpark transforms, loads to BigQuery |
| Orchestration | `airflow/dags/orders_pipeline_dag.py` | Chains the four stages above end-to-end |
| Modeling | `dbt_project/` | Staging + marts models on top of the raw BigQuery table |

## GCP Services Used

- **Pub/Sub** — real-time message ingestion
- **Cloud Storage** — raw and processed data lake
- **Apache Beam (DirectRunner)** — stream processing locally, 
  Dataflow-ready for production
- **PySpark** — batch transformations and BigQuery loading
- **BigQuery** — data warehouse (partitioned by date, 
  clustered by status)

## Local Setup

### Prerequisites
- Python 3.11
- Docker Desktop
- GCP account with billing enabled
- gcloud CLI

### Quick Start
```bash
# 1. Clone the repo
git clone https://github.com/namanbhatia11466/gcp-ecommerce-de-pipeline.git
cd gcp-ecommerce-de-pipeline

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Set up GCP credentials
# Create key.json from your GCP service account and place in root

# 5. Configure environment
cp .env.example .env
# Edit .env with your GCP project details

# 6. Start local services
docker compose up -d
```

### Run the Pipeline
```bash
# Step 1 — Publish orders to Pub/Sub
python ingestion/producer.py --num-events 20

# Step 2 — Pull messages to GCS
python scripts/pull_messages.py

# Step 3 — Run Beam processing
python pipeline/beam_pipeline.py

# Step 4 — Run Spark transform → BigQuery
python spark/transform.py

# Step 5 — Build dbt staging + marts models on top of BigQuery
export DBT_PROFILES_DIR=dbt_project   # Windows: set DBT_PROFILES_DIR=dbt_project
dbt run --project-dir dbt_project
dbt test --project-dir dbt_project
```

Or orchestrate steps 1–4 as a single Airflow DAG (`orders_pipeline`) once
`docker compose up -d` is running — trigger it from the webserver UI at
`localhost:8080`.

## Project Structure
```
gcp-ecommerce-de-pipeline/
├── ingestion/          # Pub/Sub producer
├── pipeline/           # Apache Beam pipeline
├── scripts/            # Utility scripts
├── spark/              # PySpark transforms
├── airflow/dags/       # Airflow DAG orchestrating the pipeline
├── dbt_project/        # dbt staging + marts models
├── docker-compose.yml  # Local Airflow + Spark
└── requirements.txt
```

## Design Decisions

- **DirectRunner over DataflowRunner** — runs locally at zero cost, 
  identical code deploys to Dataflow for production
- **GCS as intermediate layer** — decouples ingestion from processing, 
  enables replay of any batch
- **BigQuery partitioned by order_date** — reduces query cost by 
  scanning only relevant partitions
- **Dead letter pattern** — invalid orders routed to separate GCS 
  path instead of failing the pipeline

## Cost

Running this pipeline locally costs **$0**. The only GCP resources 
used are BigQuery and GCS which stay within the always-free tier 
for development workloads.

## Roadmap

- [x] Airflow DAG for end-to-end orchestration
- [x] dbt models for data warehouse modeling  
- [ ] GitHub Actions CI/CD pipeline
- [ ] Dataflow deployment for production streaming