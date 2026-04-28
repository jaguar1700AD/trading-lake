"""Create Gold Iceberg tables."""

import os
import sys
from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET"]
WAREHOUSE = f"s3://{BUCKET}/warehouse"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("create_gold_tables")
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


def create_ohlcv_1m(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.gold.ohlcv_1m (
            bar_start   timestamp,
            exchange    string,
            symbol      string,
            open        double,
            high        double,
            low         double,
            close       double,
            volume      bigint,
            n_trades    bigint
        )
        USING iceberg
        PARTITIONED BY (day(bar_start), exchange)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.delete.mode'       = 'copy-on-write',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created gold.ohlcv_1m")


def create_vwap_daily(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.gold.vwap_daily (
            trade_date  date,
            exchange    string,
            symbol      string,
            vwap        double,
            volume      bigint,
            n_trades    bigint
        )
        USING iceberg
        PARTITIONED BY (trade_date)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.delete.mode'       = 'copy-on-write',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created gold.vwap_daily")


def create_bid_ask_spread_1m(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.gold.bid_ask_spread_1m (
            bar_start       timestamp,
            exchange        string,
            symbol          string,
            median_spread   double,
            mid_price       double,
            n_ticks         bigint
        )
        USING iceberg
        PARTITIONED BY (day(bar_start), exchange)
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.delete.mode'       = 'copy-on-write',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created gold.bid_ask_spread_1m")


def create_event_windowed_volume(spark: SparkSession) -> None:
    spark.sql("""
        CREATE TABLE IF NOT EXISTS glue_catalog.gold.event_windowed_volume (
            event_ts        timestamp,
            event_id        string       NOT NULL,
            event_type      string,
            symbol          string,
            window_start    timestamp,
            window_end      timestamp,
            pre_volume      bigint,
            post_volume     bigint,
            realized_vol    double
        )
        USING iceberg
        PARTITIONED BY (day(event_ts))
        TBLPROPERTIES (
            'format-version'          = '2',
            'write.merge.mode'        = 'copy-on-write',
            'write.update.mode'       = 'copy-on-write',
            'write.delete.mode'       = 'copy-on-write',
            'write.parquet.compression-codec' = 'zstd'
        )
    """)
    print("Created gold.event_windowed_volume")


def main() -> None:
    spark = get_spark()
    create_ohlcv_1m(spark)
    create_vwap_daily(spark)
    create_bid_ask_spread_1m(spark)
    create_event_windowed_volume(spark)
    spark.stop()
    print("Gold tables ready.")


if __name__ == "__main__":
    main()
    print("create_gold_tables exited cleanly.")
    os._exit(0)
