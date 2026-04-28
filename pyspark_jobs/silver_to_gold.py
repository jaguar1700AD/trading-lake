"""Compute gold-layer aggregates from silver tables."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F
import pyspark.sql.types as T

BUCKET = os.environ["BUCKET"]
DATE = os.environ.get("DATE", "")
MODE = os.environ.get("MODE", "")
WAREHOUSE = f"s3://{BUCKET}/warehouse"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"silver_to_gold_{MODE}_{DATE}")
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
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def merge_into_gold(spark: SparkSession, df: DataFrame,
                    target: str, key_cols: list[str]) -> None:
    """Merge a dataframe into a gold table."""
    view_name = target.replace(".", "_").replace("glue_catalog_", "")
    df.createOrReplaceTempView(view_name)

    on_clause = " AND ".join(f"t.{c} = s.{c}" for c in key_cols)
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view_name} AS s
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"Merged into {target}.")


def compute_ohlcv(spark: SparkSession) -> None:
    """Compute 1-minute OHLCV bars from silver.trades."""
    trades = spark.sql(f"""
        SELECT exchange, symbol, exchange_ts,
               CAST(price AS DOUBLE) AS price,
               size
        FROM glue_catalog.silver.trades
        WHERE trade_date = DATE '{DATE}'
    """)

    trades = trades.withColumn(
        "bar_start",
        F.date_trunc("minute", F.col("exchange_ts"))
    )

    ohlcv = (
        trades
        .groupBy("bar_start", "exchange", "symbol")
        .agg(
            F.min(F.struct("exchange_ts", "price")).alias("_first"),
            F.max(F.struct("exchange_ts", "price")).alias("_last"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("size").cast(T.LongType()).alias("volume"),
            F.count("*").alias("n_trades"),
        )
        .select(
            F.col("bar_start"),
            F.col("exchange"),
            F.col("symbol"),
            F.col("_first.price").alias("open"),
            F.col("high"),
            F.col("low"),
            F.col("_last.price").alias("close"),
            F.col("volume"),
            F.col("n_trades"),
        )
    )

    merge_into_gold(spark, ohlcv, "glue_catalog.gold.ohlcv_1m",
                    ["bar_start", "exchange", "symbol"])


def compute_vwap(spark: SparkSession) -> None:
    """Daily VWAP from silver.trades."""
    vwap = spark.sql(f"""
        SELECT
            trade_date,
            exchange,
            symbol,
            CAST(SUM(CAST(price AS DOUBLE) * size) / SUM(size) AS DOUBLE) AS vwap,
            SUM(size)    AS volume,
            COUNT(*)     AS n_trades
        FROM glue_catalog.silver.trades
        WHERE trade_date = DATE '{DATE}'
        GROUP BY trade_date, exchange, symbol
    """)

    merge_into_gold(spark, vwap, "glue_catalog.gold.vwap_daily",
                    ["trade_date", "exchange", "symbol"])


def compute_bid_ask_spread(spark: SparkSession) -> None:
    """Compute 1-minute median bid-ask spread from silver.quotes."""
    quotes = spark.sql(f"""
        SELECT
            exchange,
            symbol,
            exchange_ts,
            CAST(bid AS DOUBLE)  AS bid,
            CAST(ask AS DOUBLE)  AS ask
        FROM glue_catalog.silver.quotes
        WHERE quote_date = DATE '{DATE}'
    """)

    quotes = (
        quotes
        .withColumn("bar_start", F.date_trunc("minute", F.col("exchange_ts")))
        .withColumn("spread_bps",
                    (F.col("ask") - F.col("bid")) / ((F.col("ask") + F.col("bid")) / 2) * 10000)
        .withColumn("mid_price",
                    (F.col("ask") + F.col("bid")) / 2)
    )

    spread = (
        quotes
        .groupBy("bar_start", "exchange", "symbol")
        .agg(
            F.percentile_approx("spread_bps", 0.5).alias("median_spread"),
            F.avg("mid_price").alias("mid_price"),
            F.count("*").alias("n_ticks"),
        )
    )

    merge_into_gold(spark, spread, "glue_catalog.gold.bid_ask_spread_1m",
                    ["bar_start", "exchange", "symbol"])


def compute_event_windowed_volume(spark: SparkSession) -> None:
    """Compute pre/post event volume and realized volatility."""
    events = spark.sql(f"""
        SELECT
            event_id,
            symbol,
            event_type,
            event_start AS event_ts
        FROM glue_catalog.silver.events
        WHERE event_date = DATE '{DATE}'
          AND event_start IS NOT NULL
    """)

    trades = spark.sql(f"""
        SELECT exchange, symbol, exchange_ts, price, size
        FROM glue_catalog.silver.trades
        WHERE trade_date BETWEEN DATE '{DATE}' - INTERVAL 1 DAY
                            AND DATE '{DATE}' + INTERVAL 1 DAY
    """)

    joined_raw = (
        trades.alias("t")
        .join(events.alias("e"), on="symbol", how="inner")
        .withColumn("window_start", F.col("e.event_ts") - F.expr("INTERVAL 1 HOUR"))
        .withColumn("window_end",   F.col("e.event_ts") + F.expr("INTERVAL 1 HOUR"))
        .filter(
            (F.col("t.exchange_ts") >= F.col("window_start")) &
            (F.col("t.exchange_ts") <= F.col("window_end"))
        )
    )

    # Normalize joined columns before aggregation.
    joined = joined_raw.select(
        F.col("t.exchange_ts").alias("exchange_ts"),
        F.col("e.event_ts").alias("event_ts"),
        F.col("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("symbol"),
        F.col("window_start"),
        F.col("window_end"),
        F.col("t.size").alias("size"),
        F.col("t.price").alias("price"),
    )

    vol_agg = (
        joined
        .withColumn("is_pre", F.col("exchange_ts") < F.col("event_ts"))
        .groupBy("event_ts", "event_id", "event_type", "symbol", "window_start", "window_end")
        .agg(
            F.sum(F.when(F.col("is_pre"), F.col("size")).otherwise(0))
             .cast(T.LongType()).alias("pre_volume"),
            F.sum(F.when(~F.col("is_pre"), F.col("size")).otherwise(0))
             .cast(T.LongType()).alias("post_volume"),
        )
    )

    # Realized volatility from 1-minute log returns.
    w_last = Window.partitionBy("symbol", "event_ts", "bar_start").orderBy(
        F.desc("exchange_ts")
    )
    bars = (
        joined
        .withColumn("bar_start", F.date_trunc("minute", F.col("exchange_ts")))
        .withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .select("symbol", "event_ts", "bar_start", "price")
    )
    w_prev = Window.partitionBy("symbol", "event_ts").orderBy("bar_start")

    rvol = (
        bars
        .withColumn("prev_price", F.lag("price").over(w_prev))
        .filter(F.col("prev_price").isNotNull())
        .withColumn("log_ret", F.log(F.col("price") / F.col("prev_price")))
        .groupBy("symbol", "event_ts")
        .agg(F.stddev("log_ret").alias("realized_vol"))
    )

    result = (
        vol_agg
        .join(rvol, on=["symbol", "event_ts"], how="left")
        .select(
            "event_ts",
            "event_id",
            "event_type",
            "symbol",
            "window_start",
            "window_end",
            "pre_volume",
            "post_volume",
            F.coalesce("realized_vol", F.lit(0.0)).alias("realized_vol"),
        )
    )

    merge_into_gold(spark, result, "glue_catalog.gold.event_windowed_volume",
                    ["event_ts", "event_id", "symbol"])


VALID_MODES = ("ohlcv", "vwap", "spread", "event_windowed")


def main() -> None:
    if not DATE:
        print("ERROR: DATE environment variable must be set.", file=sys.stderr)
        sys.exit(1)
    if MODE not in VALID_MODES:
        print(f"ERROR: MODE must be one of {VALID_MODES}, got '{MODE}'",
              file=sys.stderr)
        sys.exit(1)

    spark = get_spark()

    if MODE == "ohlcv":
        compute_ohlcv(spark)
    elif MODE == "vwap":
        compute_vwap(spark)
    elif MODE == "spread":
        compute_bid_ask_spread(spark)
    elif MODE == "event_windowed":
        compute_event_windowed_volume(spark)

    spark.stop()
    print(f"silver_to_gold [{MODE}] complete for {DATE}.")


if __name__ == "__main__":
    main()
    print("silver_to_gold exited cleanly.")
    os._exit(0)
