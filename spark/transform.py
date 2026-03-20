from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, round as spark_round,
    when, current_timestamp, lit
)
from pyspark.sql.types import DoubleType, IntegerType
import os
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET = os.getenv("GCP_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_RAW_DATASET")
INPUT_PATH = f"gs://{BUCKET}/raw/orders/*.jsonl"
BQ_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.orders"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("OrdersTransform")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1"
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.enable", "true"
        )
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        )
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        )
        .getOrCreate()
    )


def transform(spark: SparkSession):
    print(f"📥 Reading from {INPUT_PATH}...")

    # Read raw JSONL from GCS
    df = spark.read.json(INPUT_PATH)

    print(f"   Raw record count: {df.count()}")
    print(f"   Schema:")
    df.printSchema()

    # ── Transformations ──────────────────────────────────
    df_clean = (
        df
        # Cast types
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        .withColumn("user_id", col("user_id").cast(IntegerType()))

        # Derive order_date from created_at
        .withColumn("order_date", to_date(col("created_at")))

        # Round monetary values to 2 decimal places
        .withColumn("amount", spark_round(col("amount"), 2))
        .withColumn("unit_price", spark_round(col("unit_price"), 2))

        # Derive revenue = quantity * unit_price
        .withColumn(
            "revenue",
            spark_round(col("quantity") * col("unit_price"), 2)
        )

        # Categorize order size
        .withColumn(
            "order_size",
            when(col("quantity") >= 4, "bulk")
            .when(col("quantity") >= 2, "standard")
            .otherwise("single")
        )

        # Add load timestamp
        .withColumn("loaded_at", current_timestamp())

        # Drop duplicates on order_id
        .dropDuplicates(["order_id"])

        # Drop nulls on critical fields
        .dropna(subset=["order_id", "user_id", "amount"])

        # Select final columns in clean order
        .select(
            "order_id",
            "user_id",
            "product_id",
            "product_name",
            "quantity",
            "unit_price",
            "amount",
            "revenue",
            "currency",
            "status",
            "value_tier",
            "order_size",
            "country",
            "order_date",
            "created_at",
            "processed_at",
            "loaded_at"
        )
    )

    print(f"\n✅ Transformed record count: {df_clean.count()}")
    print("\n📊 Sample records:")
    df_clean.show(5, truncate=False)

    return df_clean


def load_to_bigquery(df, spark: SparkSession):
    print(f"\n📤 Loading to BigQuery: {BQ_TABLE}...")

    df.write \
        .format("bigquery") \
        .option("table", BQ_TABLE) \
        .option("temporaryGcsBucket", BUCKET) \
        .option("partitionField", "order_date") \
        .option("clusteredFields", "status,product_id") \
        .mode("append") \
        .save()

    print(f"✅ Successfully loaded to {BQ_TABLE}")


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # reduce noise

    try:
        df_clean = transform(spark)
        load_to_bigquery(df_clean, spark)
    finally:
        spark.stop()
        print("\n🏁 Spark session closed.")


if __name__ == "__main__":
    run()