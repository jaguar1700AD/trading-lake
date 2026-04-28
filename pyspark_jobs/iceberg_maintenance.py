"""Run weekly Iceberg maintenance for all pipeline tables."""

import os
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession

BUCKET = os.environ["BUCKET"]
WAREHOUSE = f"s3://{BUCKET}/warehouse"
SNAPSHOT_MAX_AGE_MS = int(os.environ.get("SNAPSHOT_MAX_AGE_MS", str(7 * 24 * 60 * 60 * 1000)))

TABLES = [
    "glue_catalog.bronze.trades_raw",
    "glue_catalog.bronze.quotes_raw",
    "glue_catalog.bronze.events_raw",
    "glue_catalog.silver.trades",
    "glue_catalog.silver.quotes",
    "glue_catalog.silver.events",
    "glue_catalog.gold.ohlcv_1m",
    "glue_catalog.gold.vwap_daily",
    "glue_catalog.gold.bid_ask_spread_1m",
    "glue_catalog.gold.event_windowed_volume",
]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("iceberg_maintenance")
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


def maintain_table(spark: SparkSession, table: str) -> None:
    print(f"\n── Maintaining {table} ──")

    print(f"  rewrite_data_files...")
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_data_files(
            table => '{table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-file-size-bytes',    '33554432',
                'max-concurrent-file-group-rewrites', '5'
            )
        )
    """).show(truncate=False)

    print(f"  rewrite_manifests...")
    spark.sql(f"""
        CALL glue_catalog.system.rewrite_manifests(table => '{table}')
    """).show(truncate=False)

    older_than = datetime.now(timezone.utc).timestamp() * 1000 - SNAPSHOT_MAX_AGE_MS
    older_than_ms = int(older_than)
    print(f"  expire_snapshots (older than {SNAPSHOT_MAX_AGE_MS // (1000*60*60*24)} days)...")
    spark.sql(f"""
        CALL glue_catalog.system.expire_snapshots(
            table => '{table}',
            older_than => TIMESTAMP '{datetime.fromtimestamp(older_than_ms/1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")}',
            retain_last => 2
        )
    """).show(truncate=False)

    print(f"  remove_orphan_files...")
    spark.sql(f"""
        CALL glue_catalog.system.remove_orphan_files(table => '{table}')
    """).show(truncate=False)

    print(f"  Done: {table}")


def main() -> None:
    spark = get_spark()
    errors = []

    for table in TABLES:
        try:
            maintain_table(spark, table)
        except Exception as exc:
            print(f"ERROR maintaining {table}: {exc}", file=sys.stderr)
            errors.append((table, str(exc)))

    spark.stop()

    if errors:
        print("\nMaintenance completed with errors:", file=sys.stderr)
        for tbl, err in errors:
            print(f"  {tbl}: {err}", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nAll tables maintained successfully.")


if __name__ == "__main__":
    main()
    print("iceberg_maintenance exited cleanly.")
    os._exit(0)
