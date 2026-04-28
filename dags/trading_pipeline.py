"""Ingest trades and quotes every 15 minutes and update intraday gold tables."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.sensors.external_task import ExternalTaskSensor

BUCKET        = Variable.get("BUCKET")
APP_ID        = Variable.get("EMR_SERVERLESS_APP_ID")
JOB_ROLE_ARN  = Variable.get("EMR_JOB_ROLE_ARN")
ATHENA_OUTPUT = Variable.get("ATHENA_OUTPUT")
JOBS_PREFIX   = f"s3://{BUCKET}/jobs"
WAREHOUSE     = f"s3://{BUCKET}/warehouse"
DEEQU_JAR     = f"s3://{BUCKET}/deequ/deequ-2.0.7-spark-3.5.jar"
PYDEEQU_VENV  = f"s3://{BUCKET}/deequ/pydeequ-venv.tar.gz"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

ICEBERG_SPARK_CONF = {
    "spark.jars":
        "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar",
    "spark.sql.extensions":
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue_catalog":
        "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl":
        "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.warehouse": WAREHOUSE,
    "spark.sql.catalog.glue_catalog.io-impl":
        "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.glue_catalog.glue.skip-archive": "true",
    "spark.sql.adaptive.enabled":                       "true",
    "spark.sql.adaptive.coalescePartitions.enabled":    "true",
    "spark.sql.adaptive.skewJoin.enabled":              "true",
    "spark.sql.session.timeZone":                       "UTC",
}

# PyDeequ dependencies for silver validation.
DEEQU_EXTRA_CONF = {
    "spark.jars": (
        "/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar"
        f",{DEEQU_JAR}"
    ),
    "spark.archives":                   f"{PYDEEQU_VENV}#pydeequ_venv",
    "spark.executorEnv.PYSPARK_PYTHON": "./pydeequ_venv/bin/python",
    "spark.executorEnv.SPARK_VERSION":  "3.5",
    "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "./pydeequ_venv/bin/python",
    "spark.emr-serverless.driverEnv.PYSPARK_PYTHON":         "./pydeequ_venv/bin/python",
    "spark.emr-serverless.driverEnv.SPARK_VERSION":           "3.5",
}


def _emr_job(task_id: str, script: str, env_vars: dict,
             extra_spark_conf: dict | None = None) -> EmrServerlessStartJobOperator:
    """Create an EMR Serverless Spark task."""
    spark_conf = {**ICEBERG_SPARK_CONF, **(extra_spark_conf or {})}
    for key, value in env_vars.items():
        # Pass job parameters through Spark conf for provider compatibility.
        spark_conf[f"spark.emr-serverless.driverEnv.{key}"] = value
        spark_conf[f"spark.executorEnv.{key}"] = value
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=APP_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{JOBS_PREFIX}/{script}",
                "sparkSubmitParameters": " ".join(
                    f"--conf {k}={v}" for k, v in spark_conf.items()
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{BUCKET}/logs/emr/{task_id}/"
                }
            }
        },
        deferrable=True,
        wait_for_completion=True,
    )


def _athena_count_check(query: str, athena_output: str,
                        min_rows: int = 1, **kwargs) -> None:
    """Run an Athena COUNT check and fail if it is below min_rows."""
    client = boto3.client("athena")

    exec_id = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": "gold"},
        ResultConfiguration={"OutputLocation": athena_output},
        WorkGroup="primary",
    )["QueryExecutionId"]

    for _ in range(60):
        resp  = client.get_query_execution(QueryExecutionId=exec_id)
        state = resp["QueryExecution"]["Status"]["State"]
        if state == "SUCCEEDED":
            break
        if state in ("FAILED", "CANCELLED"):
            raise RuntimeError(
                f"Athena query {exec_id} ended in state={state}: "
                + resp["QueryExecution"]["Status"].get("StateChangeReason", "")
            )
        time.sleep(5)
    else:
        raise TimeoutError(
            f"Athena query {exec_id} did not complete within 5 minutes"
        )

    rows  = client.get_query_results(QueryExecutionId=exec_id)["ResultSet"]["Rows"]
    count = int(rows[1]["Data"][0]["VarCharValue"])
    if count < min_rows:
        raise ValueError(
            f"Row-count assertion failed: expected >= {min_rows} rows, got {count}"
        )
    print(f"Validation passed: {count:,} rows.")


def _events_calendar_logical_date(trading_pipeline_logical_date: datetime) -> datetime:
    """Map this DAG's logical date to the matching events DAG logical date."""
    if (trading_pipeline_logical_date.hour,
        trading_pipeline_logical_date.minute) == (15, 45):
        fire_time = trading_pipeline_logical_date + timedelta(days=1)
        while fire_time.weekday() >= 5:
            fire_time += timedelta(days=1)
        fire_time = fire_time.replace(hour=9, minute=0, second=0, microsecond=0)
    else:
        fire_time = trading_pipeline_logical_date + timedelta(minutes=15)

    target = fire_time - timedelta(days=1)
    while target.weekday() >= 5:
        target -= timedelta(days=1)
    return target.replace(hour=5, minute=30, second=0, microsecond=0)


def _make_count_check(task_id: str, query_template: str,
                      min_rows: int = 1) -> PythonOperator:
    """Create an Athena row-count validation task."""
    return PythonOperator(
        task_id=task_id,
        python_callable=_athena_count_check,
        op_kwargs={
            "query":         query_template,
            "athena_output": ATHENA_OUTPUT,
            "min_rows":      min_rows,
        },
    )


with DAG(
    dag_id="trading_pipeline",
    description="15-min ingest: raw → bronze → silver → gold (ohlcv)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 15),
    schedule_interval="*/15 9-15 * * 1-5",   # Mon-Fri, 09:00-15:45 UTC
    catchup=False,
    max_active_runs=2,
    tags=["trading", "bronze", "silver", "gold"],
) as dag:

    # Airflow cron runs expose the target date via data_interval_end.
    ds = "{{ data_interval_end | ds }}"
    execution_hour = "{{ data_interval_end.strftime('%H') }}"
    branch_ts = "{{ data_interval_end.strftime('%Y%m%d_%H%M') }}"
    job_env = {"BUCKET": BUCKET, "DATE": ds, "HOUR": execution_hour}

    wait_for_trades = S3KeySensor(
        task_id="wait_for_trades",
        bucket_name=BUCKET,
        bucket_key=f"raw/trades/dt={ds}/hh={execution_hour}/*",
        wildcard_match=True,
        timeout=60 * 15,
        poke_interval=60,
        deferrable=True,
        soft_fail=True,
    )

    ingest_bronze_trades = _emr_job(
        task_id="ingest_bronze_trades",
        script="ingest_bronze_trades.py",
        env_vars=job_env,
    )

    wait_for_events_calendar = ExternalTaskSensor(
        task_id="wait_for_events_calendar",
        external_dag_id="events_calendar_daily",
        external_task_id="bronze_to_silver_events",
        execution_date_fn=_events_calendar_logical_date,
        allowed_states=["success"],
        failed_states=["failed", "skipped", "upstream_failed"],
        timeout=60 * 60 * 2,
        poke_interval=60,
        mode="reschedule",
        deferrable=True,
    )

    bronze_to_silver_trades = _emr_job(
        task_id="bronze_to_silver_trades",
        script="bronze_to_silver_trades.py",
        env_vars={"BUCKET": BUCKET, "DATE": ds, "BRANCH_TS": branch_ts},
        extra_spark_conf=DEEQU_EXTRA_CONF,
    )

    silver_to_gold_ohlcv = _emr_job(
        task_id="silver_to_gold_ohlcv",
        script="silver_to_gold.py",
        env_vars={"BUCKET": BUCKET, "DATE": ds, "MODE": "ohlcv"},
    )

    validate_gold = _make_count_check(
        task_id="validate_gold",
        query_template=(
            "SELECT COUNT(*) AS row_count "
            "FROM gold.ohlcv_1m "
            "WHERE CAST(bar_start AS DATE) = DATE '{{ data_interval_end | ds }}'"
        ),
        min_rows=1,
    )

    wait_for_quotes = S3KeySensor(
        task_id="wait_for_quotes",
        bucket_name=BUCKET,
        bucket_key=f"raw/quotes/dt={ds}/hh={execution_hour}/*",
        wildcard_match=True,
        timeout=60 * 15,
        poke_interval=60,
        deferrable=True,
        soft_fail=True,
    )

    ingest_bronze_quotes = _emr_job(
        task_id="ingest_bronze_quotes",
        script="ingest_bronze_quotes.py",
        env_vars=job_env,
    )

    bronze_to_silver_quotes = _emr_job(
        task_id="bronze_to_silver_quotes",
        script="bronze_to_silver_quotes.py",
        env_vars={"BUCKET": BUCKET, "DATE": ds, "BRANCH_TS": branch_ts},
        extra_spark_conf=DEEQU_EXTRA_CONF,
    )

    silver_to_gold_spread = _emr_job(
        task_id="silver_to_gold_spread",
        script="silver_to_gold.py",
        env_vars={"BUCKET": BUCKET, "DATE": ds, "MODE": "spread"},
    )

    validate_gold_spread = _make_count_check(
        task_id="validate_gold_spread",
        query_template=(
            "SELECT COUNT(*) AS row_count "
            "FROM gold.bid_ask_spread_1m "
            "WHERE CAST(bar_start AS DATE) = DATE '{{ data_interval_end | ds }}'"
        ),
        min_rows=1,
    )

    (
        wait_for_trades
        >> ingest_bronze_trades
        >> bronze_to_silver_trades
        >> silver_to_gold_ohlcv
        >> validate_gold
    )
    wait_for_events_calendar >> bronze_to_silver_trades

    (
        wait_for_quotes
        >> ingest_bronze_quotes
        >> bronze_to_silver_quotes
        >> silver_to_gold_spread
        >> validate_gold_spread
    )
