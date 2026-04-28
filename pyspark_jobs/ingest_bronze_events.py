"""Upsert the daily corporate-event CSV into bronze.events_raw."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

BUCKET = os.environ["BUCKET"]
DATE = os.environ.get("DATE", "")
WAREHOUSE = f"s3://{BUCKET}/warehouse"

RAW_SCHEMA = T.StructType([
    T.StructField("event_id",    T.StringType(), True),
    T.StructField("symbol",      T.StringType(), True),
    T.StructField("event_type",  T.StringType(), True),
    T.StructField("event_date",  T.StringType(), True),
    T.StructField("event_start", T.StringType(), True),
    T.StructField("event_end",   T.StringType(), True),
    T.StructField("description", T.StringType(), True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ingest_bronze_events_{DATE}")
        .config("spark.jars", "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.warehouse", WAREHOUSE)
        .config("spark.sql.catalog.glue_catalog.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.glue.skip-archive", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, s3_path: str) -> DataFrame:
    """Read raw event CSV and add bronze metadata columns."""
    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .csv(s3_path)
    )
    df = (
        df
        .withColumn("_ingest_ts",       F.lit(datetime.now(timezone.utc))
                                         .cast(T.TimestampType()))
        .withColumn("_event_load_date", F.lit(DATE).cast(T.DateType()))
    )
    return df


def drop_null_event_ids(df: DataFrame) -> DataFrame:
    """Drop rows that cannot be merged by event_id."""
    null_count = df.filter(F.col("event_id").isNull()).count()
    if null_count > 0:
        print(
            f"WARNING: dropped {null_count:,} row(s) with NULL event_id "
            f"from raw events CSV (NULL MERGE-key guard).",
            file=sys.stderr,
        )
    return df.filter(F.col("event_id").isNotNull())


def merge_into_bronze(spark: SparkSession, df: DataFrame) -> None:
    """Merge events by event_id."""
    df.createOrReplaceTempView("staged_events")

    spark.sql("""
        MERGE INTO glue_catalog.bronze.events_raw AS t
        USING staged_events AS s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main() -> None:
    if not DATE:
        print("ERROR: DATE environment variable must be set.", file=sys.stderr)
        sys.exit(1)

    s3_path = f"s3://{BUCKET}/raw/events/events_{DATE}.csv"
    print(f"Reading events from {s3_path}")

    spark = get_spark()
    df = read_raw(spark, s3_path)

    raw_count = df.count()
    print(f"Raw event records read: {raw_count:,}")

    if raw_count == 0:
        print("No event records found — nothing to ingest.")
        spark.stop()
        return

    df = drop_null_event_ids(df)
    valid_count = df.count()
    if valid_count == 0:
        print("All rows dropped (NULL event_id) — nothing to ingest.")
        spark.stop()
        return

    merge_into_bronze(spark, df)
    print(f"Merge complete for {DATE}: {valid_count:,} record(s) staged.")
    spark.stop()


if __name__ == "__main__":
    main()
    print("ingest_bronze_events exited cleanly.")
    os._exit(0)
