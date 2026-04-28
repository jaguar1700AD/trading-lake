"""Promote bronze events to silver after validation."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

BUCKET = os.environ["BUCKET"]
DATE = os.environ.get("DATE", "")
WAREHOUSE = f"s3://{BUCKET}/warehouse"
DEEQU_JAR = f"s3://{BUCKET}/deequ/deequ-2.0.7-spark-3.5.jar"
PYDEEQU_VENV = f"s3://{BUCKET}/deequ/pydeequ-venv.tar.gz"

# Keep in sync with generator/generate_synthetic.py.
VALID_EVENT_TYPES = ["EARNINGS", "DIVIDEND", "SPLIT", "GUIDANCE", "BUYBACK"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"bronze_to_silver_events_{DATE}")
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
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession) -> DataFrame:
    """Read bronze events for the target load date."""
    return spark.sql(f"""
        SELECT *
        FROM glue_catalog.bronze.events_raw
        WHERE _event_load_date = DATE '{DATE}'
    """)


def transform(bronze_df: DataFrame) -> DataFrame:
    """Cast, deduplicate, and order columns for silver.events."""
    from pyspark.sql.window import Window

    dedup_window = Window.partitionBy("event_id", "event_date").orderBy(
        F.col("_ingest_ts").desc()
    )

    return (
        bronze_df
        .withColumn("event_date",  F.to_date("event_date", "yyyy-MM-dd"))
        .withColumn("event_start", F.to_timestamp("event_start"))
        .withColumn("event_end",   F.to_timestamp("event_end"))
        .withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
        .select(
            "event_id", "symbol", "event_type", "event_date",
            "event_start", "event_end", "description",
            "_ingest_ts",
        )
    )


def validate_required_fields(df: DataFrame) -> None:
    """Fail fast if required silver.events fields are null."""
    required = ["event_id", "symbol", "event_type", "event_date"]
    null_pred = None
    for c in required:
        cond = F.col(c).isNull()
        null_pred = cond if null_pred is None else null_pred | cond

    bad = df.filter(null_pred).count()
    if bad > 0:
        raise ValueError(
            f"silver.events validation failed: {bad} row(s) have NULL in one "
            f"of the required fields {required}.  Aborting MERGE."
        )


def run_deequ_checks(spark: SparkSession, df: DataFrame) -> bool:
    """Run PyDeequ checks on transformed events."""
    try:
        import pydeequ
        from pydeequ.checks import Check, CheckLevel
        from pydeequ.verification import VerificationSuite, VerificationResult

        check = (
            Check(spark, CheckLevel.Error, "silver_events_quality")
            .isComplete("event_id")
            .isComplete("symbol")
            .isComplete("event_type")
            .isComplete("event_date")
            .isContainedIn("event_type", VALID_EVENT_TYPES)
            .isComplete("event_start")
            .satisfies("event_end > event_start", "event_end_gt_start",
                       lambda x: x >= 1.0)
            .hasUniqueness(
                ["event_id", "event_date"],
                lambda x: x >= 1.0,
                "silver.events MERGE-key uniqueness must be 100%",
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
            "ERROR: pydeequ not available — cannot validate data quality. "
            "Failing job.",
            file=sys.stderr,
        )
        print(f"ImportError detail: {exc}", file=sys.stderr)
        print(f"Python executable: {sys.executable}", file=sys.stderr)
        print(f"SPARK_VERSION env: {os.environ.get('SPARK_VERSION')}", file=sys.stderr)
        print(f"PYSPARK_PYTHON env: {os.environ.get('PYSPARK_PYTHON')}", file=sys.stderr)
        print(f"PYSPARK_DRIVER_PYTHON env: {os.environ.get('PYSPARK_DRIVER_PYTHON')}", file=sys.stderr)
        return False


def merge_into_silver(spark: SparkSession, df: DataFrame) -> None:
    """MERGE INTO silver.events ON (event_id, event_date)."""
    df.createOrReplaceTempView("staged_events")

    spark.sql("""
        MERGE INTO glue_catalog.silver.events AS t
        USING staged_events AS s
        ON  t.event_id   = s.event_id
        AND t.event_date = s.event_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("Merge into silver.events complete.")


def main() -> None:
    if not DATE:
        print("ERROR: DATE environment variable must be set.", file=sys.stderr)
        sys.exit(1)

    spark = get_spark()

    bronze_df = read_bronze(spark)
    bronze_count = bronze_df.count()
    print(f"Bronze event records for load date {DATE}: {bronze_count:,}")

    if bronze_count == 0:
        print("No bronze records — nothing to promote.")
        spark.stop()
        return

    transformed_df = transform(bronze_df)
    deduped_count = transformed_df.count()
    if deduped_count != bronze_count:
        print(f"Deduped {bronze_count - deduped_count} duplicate row(s) "
              f"on (event_id, event_date).")

    validate_required_fields(transformed_df)

    if not run_deequ_checks(spark, transformed_df):
        raise RuntimeError(
            f"Deequ checks failed for {DATE} — silver.events not updated."
        )

    merge_into_silver(spark, transformed_df)
    spark.stop()


if __name__ == "__main__":
    main()
    print("bronze_to_silver_events exited cleanly.")
    os._exit(0)
