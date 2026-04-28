"""Compute daily gold aggregates after market close."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

BUCKET        = Variable.get("BUCKET")
APP_ID        = Variable.get("EMR_SERVERLESS_APP_ID")
JOB_ROLE_ARN  = Variable.get("EMR_JOB_ROLE_ARN")
ATHENA_OUTPUT = Variable.get("ATHENA_OUTPUT")
JOBS_PREFIX   = f"s3://{BUCKET}/jobs"
WAREHOUSE     = f"s3://{BUCKET}/warehouse"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=15),
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


def _emr_job(task_id: str, mode: str, ds_template: str) -> EmrServerlessStartJobOperator:
    env_vars = {
        "BUCKET": BUCKET,
        "DATE":   ds_template,
        "MODE":   mode,
    }
    spark_conf = dict(ICEBERG_SPARK_CONF)
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
                "entryPoint": f"{JOBS_PREFIX}/silver_to_gold.py",
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
    dag_id="gold_daily",
    description="Daily gold aggregates after market close (23:00 UTC)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 15),
    schedule_interval="0 23 * * 1-5",   # Mon-Fri at 23:00 UTC (after last trading_pipeline slot)
    catchup=False,
    max_active_runs=1,
    tags=["trading", "gold"],
) as dag:

    # Airflow cron runs expose the target date via data_interval_end.
    ds = "{{ data_interval_end | ds }}"

    silver_to_gold_vwap = _emr_job(
        task_id="silver_to_gold_vwap",
        mode="vwap",
        ds_template=ds,
    )

    silver_to_gold_event_windowed = _emr_job(
        task_id="silver_to_gold_event_windowed",
        mode="event_windowed",
        ds_template=ds,
    )

    validate_vwap = _make_count_check(
        task_id="validate_vwap",
        query_template=(
            "SELECT COUNT(*) AS row_count "
            "FROM gold.vwap_daily "
            "WHERE trade_date = DATE '{{ data_interval_end | ds }}'"
        ),
        min_rows=1,
    )

    validate_event_windowed = _make_count_check(
        task_id="validate_event_windowed",
        query_template=(
            "SELECT COUNT(*) AS row_count "
            "FROM gold.event_windowed_volume "
            "WHERE CAST(event_ts AS DATE) = DATE '{{ data_interval_end | ds }}'"
        ),
        min_rows=0,
    )

    silver_to_gold_vwap            >> validate_vwap
    silver_to_gold_event_windowed  >> validate_event_windowed
