"""Load the daily corporate-event calendar into bronze and silver tables."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

BUCKET       = Variable.get("BUCKET")
APP_ID       = Variable.get("EMR_SERVERLESS_APP_ID")
JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN")
JOBS_PREFIX  = f"s3://{BUCKET}/jobs"
WAREHOUSE    = f"s3://{BUCKET}/warehouse"
DEEQU_JAR    = f"s3://{BUCKET}/deequ/deequ-2.0.7-spark-3.5.jar"
PYDEEQU_VENV = f"s3://{BUCKET}/deequ/pydeequ-venv.tar.gz"

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
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


with DAG(
    dag_id="events_calendar_daily",
    description="Daily event calendar: raw CSV → bronze.events_raw → silver.events",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 15),
    schedule_interval="30 5 * * 1-5",   # Mon-Fri at 05:30 UTC (before trading_pipeline)
    catchup=False,
    max_active_runs=1,
    tags=["trading", "bronze", "silver", "events"],
) as dag:

    # Airflow cron runs expose the target date via data_interval_end.
    ds = "{{ data_interval_end | ds }}"
    job_env = {"BUCKET": BUCKET, "DATE": ds}

    events_sensor = S3KeySensor(
        task_id="events_sensor",
        bucket_name=BUCKET,
        bucket_key=f"raw/events/events_{ds}.csv",
        timeout=60 * 30,
        poke_interval=60,
        mode="reschedule",
        deferrable=True,
    )

    ingest_bronze_events = _emr_job(
        task_id="ingest_bronze_events",
        script="ingest_bronze_events.py",
        env_vars=job_env,
    )

    bronze_to_silver_events = _emr_job(
        task_id="bronze_to_silver_events",
        script="bronze_to_silver_events.py",
        env_vars=job_env,
        extra_spark_conf=DEEQU_EXTRA_CONF,
    )

    events_sensor >> ingest_bronze_events >> bronze_to_silver_events
