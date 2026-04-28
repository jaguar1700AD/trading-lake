"""Promote bronze trades to silver with WAP validation."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

BUCKET    = os.environ["BUCKET"]
DATE      = os.environ.get("DATE", "")
BRANCH_TS = os.environ.get("BRANCH_TS", "")
WAREHOUSE = f"s3://{BUCKET}/warehouse"
DEEQU_JAR = f"s3://{BUCKET}/deequ/deequ-2.0.7-spark-3.5.jar"
PYDEEQU_VENV = f"s3://{BUCKET}/deequ/pydeequ-venv.tar.gz"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"bronze_to_silver_trades_{BRANCH_TS}")
        .config("spark.jars",
                f"/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar,{DEEQU_JAR}")
        .config("spark.archives", f"{PYDEEQU_VENV}#pydeequ_venv")
        .config("spark.executorEnv.PYSPARK_PYTHON", "./pydeequ_venv/bin/python")
        .config("spark.executorEnv.SPARK_VERSION", "3.5")
        .config("spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON",
                "./pydeequ_venv/bin/python")
        .config("spark.emr-serverless.driverEnv.PYSPARK_PYTHON",
                "./pydeequ_venv/bin/python")
        .config("spark.emr-serverless.driverEnv.SPARK_VERSION", "3.5")
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


def read_bronze(spark: SparkSession) -> DataFrame:
    """Read bronze trades for the target trading date."""
    return spark.sql(f"""
        SELECT *
        FROM glue_catalog.bronze.trades_raw
        WHERE _trade_date = DATE '{DATE}'
    """)


def transform(bronze_df: DataFrame, events_df: DataFrame) -> DataFrame:
    """Cast, deduplicate, and enrich trades."""
    from pyspark.sql.window import Window

    dedup_window = Window.partitionBy(
        "exchange", "symbol", "exchange_ts", "sequence_num"
    ).orderBy("_ingest_ts")

    silver_df = (
        bronze_df
        .withColumn("exchange_ts",    F.to_timestamp("exchange_ts"))
        .withColumn("trade_date",     F.to_date("exchange_ts"))
        .withColumn("price",          F.col("price").cast(T.DecimalType(18, 6)))
        .withColumn("_rn",            F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Avoid duplicating trade rows when multiple events exist for a symbol-day.
    events_dedup_window = Window.partitionBy("symbol", "event_date").orderBy(
        "event_start", "event_id"
    )
    events_slim = (
        events_df
        .withColumn("_ev_rn", F.row_number().over(events_dedup_window))
        .filter(F.col("_ev_rn") == 1)
        .select(
            F.col("symbol").alias("ev_symbol"),
            F.col("event_date").alias("ev_date"),
            F.col("event_id"),
            F.col("event_type"),
        )
    )

    silver_df = (
        silver_df
        .join(
            events_slim,
            (silver_df["symbol"] == events_slim["ev_symbol"]) &
            (silver_df["trade_date"] == events_slim["ev_date"]),
            how="left",
        )
        .drop("ev_symbol", "ev_date")
    )

    return silver_df.select(
        "exchange", "symbol", "exchange_ts", "sequence_num",
        "trade_date", "price", "size", "side", "trade_id",
        "event_id", "event_type",
        "_ingest_ts",
    )


def run_deequ_checks(spark: SparkSession, df: DataFrame) -> bool:
    """Run PyDeequ checks on transformed trades."""
    try:
        import pydeequ
        from pydeequ.checks import Check, CheckLevel
        from pydeequ.verification import VerificationSuite, VerificationResult

        check = (
            Check(spark, CheckLevel.Error, "silver_trades_quality")
            .isComplete("exchange")
            .isComplete("symbol")
            .isComplete("exchange_ts")
            .isComplete("sequence_num")
            .isContainedIn("side", ["BUY", "SELL", "MID"])
            .isNonNegative("size")
            .isNonNegative("price")
            .hasUniqueness(
                ["exchange", "symbol", "exchange_ts", "sequence_num"],
                lambda x: x >= 0.9999,
                "Dedup uniqueness must be >= 99.99%",
            )
        )

        result = VerificationSuite(spark).onData(df).addCheck(check).run()
        outcome = VerificationResult.checkResultsAsDataFrame(spark, result)
        outcome.show(truncate=False)

        failed = outcome.filter(F.col("constraint_status") != "Success").count()
        if failed > 0:
            print(f"DEEQU FAILED: {failed} constraint(s) violated.", file=sys.stderr)
            return False
        print("Deequ checks passed.")
        return True
    except ImportError as exc:
        print(
            "ERROR: pydeequ not available — cannot validate data quality. Failing job.",
            file=sys.stderr,
        )
        print(f"ImportError detail: {exc}", file=sys.stderr)
        print(f"Python executable: {sys.executable}", file=sys.stderr)
        print(f"SPARK_VERSION env: {os.environ.get('SPARK_VERSION')}", file=sys.stderr)
        print(f"PYSPARK_PYTHON env: {os.environ.get('PYSPARK_PYTHON')}", file=sys.stderr)
        print(f"PYSPARK_DRIVER_PYTHON env: {os.environ.get('PYSPARK_DRIVER_PYTHON')}", file=sys.stderr)
        return False


def wap_commit(spark: SparkSession, df: DataFrame) -> None:
    """Write, validate, and publish silver.trades."""
    target = "glue_catalog.silver.trades"
    branch = f"audit_{BRANCH_TS}"

    spark.sql(f"ALTER TABLE {target} CREATE OR REPLACE BRANCH {branch}")

    spark.conf.set("spark.wap.branch", branch)

    df.createOrReplaceTempView("silver_trades_staged")
    spark.sql(f"""
        MERGE INTO {target} AS t
        USING silver_trades_staged AS s
        ON  t.exchange    = s.exchange
        AND t.symbol      = s.symbol
        AND t.exchange_ts = s.exchange_ts
        AND t.sequence_num = s.sequence_num
        WHEN NOT MATCHED THEN INSERT *
        WHEN MATCHED THEN UPDATE SET *
    """)

    audit_df = spark.sql(f"""
        SELECT * FROM {target} VERSION AS OF '{branch}'
        WHERE trade_date = DATE '{DATE}'
    """)

    if not run_deequ_checks(spark, audit_df):
        spark.conf.unset("spark.wap.branch")
        spark.sql(f"ALTER TABLE {target} DROP BRANCH {branch}")
        raise RuntimeError(f"Deequ checks failed for {DATE} — not publishing to main.")

    spark.conf.unset("spark.wap.branch")
    spark.sql(f"CALL glue_catalog.system.fast_forward('{target}', 'main', '{branch}')")

    spark.sql(f"ALTER TABLE {target} DROP BRANCH {branch}")
    print("WAP commit complete — silver.trades updated.")


def main() -> None:
    if not DATE:
        print("ERROR: DATE environment variable must be set.", file=sys.stderr)
        sys.exit(1)
    if not BRANCH_TS:
        print("ERROR: BRANCH_TS environment variable must be set.", file=sys.stderr)
        sys.exit(1)

    spark = get_spark()

    # Reprocess the full day so late trade corrections can update silver rows.
    bronze_df = read_bronze(spark)
    count = bronze_df.count()
    print(f"Bronze records for {DATE}: {count:,}")

    if count == 0:
        print("No new bronze records — nothing to promote.")
        spark.stop()
        return

    events_df = spark.sql(f"""
        SELECT event_id, symbol, event_type, event_date, event_start
        FROM glue_catalog.silver.events
        WHERE event_date = DATE '{DATE}'
    """)

    transformed_df = transform(bronze_df, events_df)
    wap_commit(spark, transformed_df)

    spark.stop()


if __name__ == "__main__":
    main()
    print("bronze_to_silver_trades exited cleanly.")
    os._exit(0)
