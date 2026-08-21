from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator

from airflow import DAG

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="orders_pipeline",
    description="Orders pipeline: Pub/Sub -> GCS landing -> Beam validate/enrich -> Spark transform -> BigQuery",
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["orders", "gcp"],
) as dag:
    produce_orders = BashOperator(
        task_id="produce_orders",
        bash_command="python /opt/airflow/ingestion/producer.py --num-events 20",
    )

    pull_to_gcs = BashOperator(
        task_id="pull_to_gcs",
        bash_command="python /opt/airflow/scripts/pull_messages.py",
    )

    beam_validate_enrich = BashOperator(
        task_id="beam_validate_enrich",
        bash_command="python /opt/airflow/pipeline/beam_pipeline.py",
    )

    spark_transform_load = BashOperator(
        task_id="spark_transform_load",
        bash_command="python /opt/airflow/spark/transform.py",
    )

    produce_orders >> pull_to_gcs >> beam_validate_enrich >> spark_transform_load
