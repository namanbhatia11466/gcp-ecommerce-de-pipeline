import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import DoubleType, IntegerType

sys.stdout.reconfigure(encoding="utf-8")
load_dotenv()

LOCAL_MODE = os.getenv("LOCAL_MODE", "false").lower() == "true"
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "local-dev" if LOCAL_MODE else None)
BUCKET = os.getenv("GCP_BUCKET_NAME")
BQ_DATASET = os.getenv("BQ_RAW_DATASET")
BQ_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.orders"

if LOCAL_MODE:
    _DATA_ROOT = Path(__file__).resolve().parent.parent / "data" / "gcs"
    _WAREHOUSE_ROOT = Path(__file__).resolve().parent.parent / "data" / "warehouse"
    INPUT_PATH = str(_DATA_ROOT / "raw" / "orders" / "*.jsonl")
    LOCAL_OUTPUT_PATH = str(_WAREHOUSE_ROOT / "orders")
else:
    INPUT_PATH = f"gs://{BUCKET}/raw/orders/*.jsonl"


def create_spark_session():
    builder = SparkSession.builder.appName("OrdersTransform").master("local[*]")

    if LOCAL_MODE:
        # No GCS/BigQuery connector needed - reads/writes local paths only,
        # so no reason to pull in and configure those jars.
        return builder.getOrCreate()

    return (
        builder.config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1",
        )
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
        )
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .getOrCreate()
    )


def transform(spark: SparkSession):
    print(f"📥 Reading from {INPUT_PATH}...")

    if LOCAL_MODE:
        # Expand the glob in Python rather than handing Spark a wildcard:
        # Hadoop's local FileSystem glob resolution needs winutils.exe on
        # Windows, which we're deliberately not pulling in for a local-only
        # demo path. Explicit file paths skip that code path entirely.
        input_files = [str(p) for p in Path(INPUT_PATH).parent.glob(Path(INPUT_PATH).name)]
        if not input_files:
            raise SystemExit(
                f"No input files found matching {INPUT_PATH} - run the beam stage first."
            )
        df = spark.read.json(input_files)
    else:
        df = spark.read.json(INPUT_PATH)

    print(f"   Raw record count: {df.count()}")
    print("   Schema:")
    df.printSchema()

    # ── Transformations ──────────────────────────────────
    df_clean = (
        df
        # Cast types (customer_id is a real Olist hash, not numeric - no cast)
        .withColumn("amount", col("amount").cast(DoubleType()))
        .withColumn("unit_price", col("unit_price").cast(DoubleType()))
        .withColumn("freight_value", col("freight_value").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(IntegerType()))
        # Derive order_date from created_at
        .withColumn("order_date", to_date(col("created_at")))
        # Round monetary values to 2 decimal places
        .withColumn("amount", spark_round(col("amount"), 2))
        .withColumn("unit_price", spark_round(col("unit_price"), 2))
        .withColumn("freight_value", spark_round(col("freight_value"), 2))
        # Derive revenue = amount + freight (total actually charged for the line)
        .withColumn("revenue", spark_round(col("amount") + col("freight_value"), 2))
        # Categorize order size
        .withColumn(
            "order_size",
            when(col("quantity") >= 4, "bulk")
            .when(col("quantity") >= 2, "standard")
            .otherwise("single"),
        )
        # Add load timestamp
        .withColumn("loaded_at", current_timestamp())
        # Drop duplicates on the natural key - order_id alone isn't unique,
        # an order can have multiple product lines
        .dropDuplicates(["order_id", "product_id"])
        # Drop nulls on critical fields
        .dropna(subset=["order_id", "customer_id", "amount"])
        # Select final columns in clean order
        .select(
            "order_id",
            "customer_id",
            "product_id",
            "product_category",
            "quantity",
            "unit_price",
            "amount",
            "revenue",
            "freight_value",
            "currency",
            "status",
            "value_tier",
            "order_size",
            "payment_type",
            "customer_state",
            "order_date",
            "created_at",
            "processed_at",
            "loaded_at",
        )
    )

    print(f"\n✅ Transformed record count: {df_clean.count()}")
    print("\n📊 Sample records:")
    df_clean.show(5, truncate=False)

    return df_clean


def load_to_bigquery(df, spark: SparkSession):
    print(f"\n📤 Loading to BigQuery: {BQ_TABLE}...")

    df.write.format("bigquery").option("table", BQ_TABLE).option(
        "temporaryGcsBucket", BUCKET
    ).option("partitionField", "order_date").option("clusteredFields", "status,product_id").mode(
        "append"
    ).save()

    print(f"✅ Successfully loaded to {BQ_TABLE}")


def load_to_local_parquet(df):
    # BigQuery has no local emulator that works reliably with Spark's
    # BigQuery Storage Write API connector, so LOCAL_MODE lands the same
    # transformed data as partitioned Parquet instead - same transform
    # logic, only the sink differs. dbt's staging/marts models still
    # require a real BigQuery connection.
    #
    # Written via pandas/pyarrow rather than df.write.parquet(): Spark's
    # own writer goes through Hadoop's FileOutputCommitter, which needs
    # winutils.exe on Windows (no official Apache build exists, and we're
    # not pulling in an unofficial binary just for a local demo path).
    # LOCAL_MODE data is small enough that collecting to pandas is fine.
    print(f"\n📤 Writing Parquet to {LOCAL_OUTPUT_PATH} (partitioned by order_date)...")

    pdf = df.toPandas()
    for order_date, group in pdf.groupby("order_date"):
        partition_dir = Path(LOCAL_OUTPUT_PATH) / f"order_date={order_date}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        existing = list(partition_dir.glob("part-*.parquet"))
        group.drop(columns=["order_date"]).to_parquet(
            partition_dir / f"part-{len(existing):04d}.parquet", index=False
        )

    print(f"✅ Successfully wrote to {LOCAL_OUTPUT_PATH}")


def run():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")  # reduce noise

    try:
        df_clean = transform(spark)
        if LOCAL_MODE:
            load_to_local_parquet(df_clean)
        else:
            load_to_bigquery(df_clean, spark)
    finally:
        spark.stop()
        print("\n🏁 Spark session closed.")


if __name__ == "__main__":
    run()
