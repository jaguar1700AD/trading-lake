"""Create Bronze Iceberg tables."""

import os
import sys
from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET"]
WAREHOUSE = f"s3://{BUCKET}/warehouse"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("create_bronze_tables")
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
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_trades_raw(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.trades_raw (
            exchange        string,
            symbol          string,
            exchange_ts     string,
            price           double,
            size            bigint,
            sequence_num    bigint,
            side            string,
            trade_id        string,
            _ingest_ts      timestamp,
            _trade_date     date
        )
        USING iceberg
        PARTITIONED BY (_trade_date)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'merge-on-read',
            'write.update.mode'       = 'merge-on-read',
            'write.delete.mode'       = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created bronze.trades_raw")


def create_quotes_raw(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.quotes_raw (
            exchange        string,
            symbol          string,
            exchange_ts     string,
            bid             double,
            bid_size        bigint,
            ask             double,
            ask_size        bigint,
            sequence_num    bigint,
            _ingest_ts      timestamp,
            _quote_date     date
        )
        USING iceberg
        PARTITIONED BY (_quote_date)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'merge-on-read',
            'write.update.mode'       = 'merge-on-read',
            'write.delete.mode'       = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created bronze.quotes_raw")


def create_events_raw(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.bronze.events_raw (
            event_id          string,
            symbol            string,
            event_type        string,
            event_date        string,
            event_start       string,
            event_end         string,
            description       string,
            _ingest_ts        timestamp,
            _event_load_date  date
        )
        USING iceberg
        PARTITIONED BY (_event_load_date)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'merge-on-read',
            'write.update.mode'       = 'merge-on-read',
            'write.delete.mode'       = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created bronze.events_raw")


def main() -> None:
    spark = get_spark()
    create_trades_raw(spark)
    create_quotes_raw(spark)
    create_events_raw(spark)
    spark.stop()
    print("Bronze tables ready.")


if __name__ == "__main__":
    main()
    print("create_bronze_tables exited cleanly.")
    os._exit(0)
