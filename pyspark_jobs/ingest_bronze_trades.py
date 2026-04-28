"""Upsert raw trade JSONL files into bronze.trades_raw."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

BUCKET = os.environ["BUCKET"]
DATE = os.environ.get("DATE", "")
HOUR = os.environ.get("HOUR", "")
WAREHOUSE = f"s3://{BUCKET}/warehouse"

RAW_SCHEMA = T.StructType([
    T.StructField("exchange",     T.StringType(),  True),
    T.StructField("symbol",       T.StringType(),  True),
    T.StructField("exchange_ts",  T.StringType(),  True),
    T.StructField("price",        T.DoubleType(),  True),
    T.StructField("size",         T.LongType(),    True),
    T.StructField("sequence_num", T.LongType(),    True),
    T.StructField("side",         T.StringType(),  True),
    T.StructField("trade_id",     T.StringType(),  True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"ingest_bronze_trades_{DATE}_{HOUR}")
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


def read_raw(spark: SparkSession, s3_prefix: str) -> DataFrame:
    """Read raw trade files and add bronze metadata columns."""
    df = (
        spark.read
        .schema(RAW_SCHEMA)
        .option("compression", "gzip")
        .json(s3_prefix)
    )
    df = (
        df
        .withColumn("_ingest_ts",  F.lit(datetime.now(timezone.utc))
                                    .cast(T.TimestampType()))
        .withColumn("_trade_date", F.lit(DATE).cast(T.DateType()))
    )
    return df


def merge_into_bronze(spark: SparkSession, df: DataFrame) -> None:
    """Merge trades by natural key."""
    df.createOrReplaceTempView("staged_trades")

    spark.sql("""
        MERGE INTO glue_catalog.bronze.trades_raw AS t
        USING staged_trades AS s
        ON  t.exchange     = s.exchange
        AND t.symbol       = s.symbol
        AND t.exchange_ts  = s.exchange_ts
        AND t.sequence_num = s.sequence_num
        AND t.trade_id     = s.trade_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def main() -> None:
    if not DATE or not HOUR:
        print("ERROR: DATE and HOUR environment variables must be set.", file=sys.stderr)
        sys.exit(1)

    s3_prefix = f"s3://{BUCKET}/raw/trades/dt={DATE}/hh={HOUR}/"
    print(f"Reading trades from {s3_prefix}")

    spark = get_spark()
    df = read_raw(spark, s3_prefix)

    count = df.count()
    print(f"Records read: {count:,}")

    if count == 0:
        print("No records found — nothing to ingest.")
        spark.stop()
        return

    merge_into_bronze(spark, df)
    print(f"Merge complete for {DATE} hour {HOUR}.")
    spark.stop()


if __name__ == "__main__":
    main()
    print("ingest_bronze_trades exited cleanly.")
    os._exit(0)
