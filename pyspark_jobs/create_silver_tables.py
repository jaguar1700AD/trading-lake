"""Create Silver Iceberg tables."""

import os
import sys
from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET"]
WAREHOUSE = f"s3://{BUCKET}/warehouse"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("create_silver_tables")
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


def create_silver_trades(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.trades (
            exchange        string       NOT NULL,
            symbol          string       NOT NULL,
            exchange_ts     timestamp    NOT NULL,
            sequence_num    bigint       NOT NULL,
            trade_date      date,
            price           decimal(18,6),
            size            bigint,
            side            string,
            trade_id        string,
            event_id        string,
            event_type      string,
            _ingest_ts      timestamp
        )
        USING iceberg
        PARTITIONED BY (day(exchange_ts), exchange)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.wap.enabled'       = 'true',
            'write.merge.mode'        = 'merge-on-read',
            'write.update.mode'       = 'merge-on-read',
            'write.delete.mode'       = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created silver.trades")


def create_silver_quotes(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.quotes (
            exchange        string       NOT NULL,
            symbol          string       NOT NULL,
            exchange_ts     timestamp    NOT NULL,
            sequence_num    bigint       NOT NULL,
            quote_date      date,
            bid             decimal(18,6),
            bid_size        bigint,
            ask             decimal(18,6),
            ask_size        bigint,
            _ingest_ts      timestamp
        )
        USING iceberg
        PARTITIONED BY (day(exchange_ts), exchange)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.wap.enabled'       = 'true',
            'write.merge.mode'        = 'merge-on-read',
            'write.update.mode'       = 'merge-on-read',
            'write.delete.mode'       = 'merge-on-read',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created silver.quotes")


def create_silver_events(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.silver.events (
            event_id        string   NOT NULL,
            symbol          string   NOT NULL,
            event_type      string   NOT NULL,
            event_date      date     NOT NULL,
            event_start     timestamp,
            event_end       timestamp,
            description     string,
            _ingest_ts      timestamp
        )
        USING iceberg
        PARTITIONED BY (day(event_date))
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.delete.mode'       = 'copy-on-write',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created silver.events")


def main() -> None:
    spark = get_spark()
    create_silver_trades(spark)
    create_silver_quotes(spark)
    create_silver_events(spark)
    spark.stop()
    print("Silver tables ready.")


if __name__ == "__main__":
    main()
    print("create_silver_tables exited cleanly.")
    os._exit(0)
