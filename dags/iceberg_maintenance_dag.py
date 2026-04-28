"""Run weekly Iceberg table maintenance."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

BUCKET       = Variable.get("BUCKET")
APP_ID       = Variable.get("EMR_SERVERLESS_APP_ID")
JOB_ROLE_ARN = Variable.get("EMR_JOB_ROLE_ARN")
JOBS_PREFIX  = f"s3://{BUCKET}/jobs"
WAREHOUSE    = f"s3://{BUCKET}/warehouse"

SNAPSHOT_MAX_AGE_MS = str(7 * 24 * 60 * 60 * 1000)

DEFAULT_ARGS = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=30),
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
    "spark.executor.memory": "8g",
    "spark.executor.cores":  "4",
}


with DAG(
    dag_id="iceberg_maintenance",
    description="Weekly Iceberg table maintenance (compact, expire, cleanup)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 21),
    schedule_interval="0 2 * * 0",     # Sundays at 02:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["trading", "maintenance", "iceberg"],
) as dag:

    env_vars = {
        "BUCKET":              BUCKET,
        "SNAPSHOT_MAX_AGE_MS": SNAPSHOT_MAX_AGE_MS,
    }
    spark_conf = dict(ICEBERG_SPARK_CONF)
    for key, value in env_vars.items():
        # Pass job parameters through Spark conf for provider compatibility.
        spark_conf[f"spark.emr-serverless.driverEnv.{key}"] = value
        spark_conf[f"spark.executorEnv.{key}"] = value

    run_maintenance = EmrServerlessStartJobOperator(
        task_id="run_iceberg_maintenance",
        application_id=APP_ID,
        execution_role_arn=JOB_ROLE_ARN,
        job_driver={
            "sparkSubmit": {
                "entryPoint": f"{JOBS_PREFIX}/iceberg_maintenance.py",
                "sparkSubmitParameters": " ".join(
                    f"--conf {k}={v}" for k, v in spark_conf.items()
                ),
            }
        },
        configuration_overrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {
                    "logUri": f"s3://{BUCKET}/logs/emr/iceberg_maintenance/"
                }
            },
            "applicationConfiguration": [
                {
                    "classification": "spark-defaults",
                    "properties": {
                        "spark.dynamicAllocation.enabled":    "true",
                        "spark.dynamicAllocation.minExecutors": "2",
                        "spark.dynamicAllocation.maxExecutors": "20",
                    }
                }
            ],
        },
        deferrable=True,
        wait_for_completion=True,
    )
