# AWS Trading Lakehouse

This project is a synthetic trading data lakehouse built on AWS. It generates raw trade, quote, and corporate-event data, lands it in Amazon S3, and processes it through Bronze, Silver, and Gold Apache Iceberg tables using EMR Serverless, Spark, AWS Glue Data Catalog, Amazon MWAA, and Athena.

All setup commands in this README assume a Linux environment with a Bash shell.

## Architecture

The pipeline follows a medallion architecture:

```text
Synthetic data generator
  -> S3 raw files
  -> Bronze Iceberg tables
  -> Silver typed and quality-gated Iceberg tables
  -> Gold analytics tables
  -> Athena queries and validation
```

Core AWS services:

| Service | Purpose |
| --- | --- |
| Amazon S3 | Raw data, Iceberg warehouse, Spark jobs, Airflow DAGs, logs, and Athena results |
| EMR Serverless | Runs PySpark jobs with Spark 3.5 on EMR 7.1.0 |
| AWS Glue Data Catalog | Catalog for Bronze, Silver, and Gold Iceberg tables |
| Apache Iceberg | Lakehouse table format with MERGE support and table maintenance |
| Amazon MWAA | Managed Airflow orchestration |
| Athena | Query and validation layer |
| PyDeequ | Data quality gates for Silver tables |

## Repository Layout

```text
.
|-- dags/
|   |-- events_calendar_daily.py
|   |-- gold_daily.py
|   |-- iceberg_maintenance_dag.py
|   `-- trading_pipeline.py
|-- generator/
|   `-- generate_synthetic.py
|-- pyspark_jobs/
|   |-- create_bronze_tables.py
|   |-- create_silver_tables.py
|   |-- create_gold_tables.py
|   |-- ingest_bronze_trades.py
|   |-- ingest_bronze_quotes.py
|   |-- ingest_bronze_events.py
|   |-- bronze_to_silver_trades.py
|   |-- bronze_to_silver_quotes.py
|   |-- bronze_to_silver_events.py
|   |-- silver_to_gold.py
|   `-- iceberg_maintenance.py
|-- DATA_PIPELINE_WALKTHROUGH.md
`-- README.md
```

For a more detailed explanation of the pipeline behavior, DAG flow, raw feeds, and table transformations, see `DATA_PIPELINE_WALKTHROUGH.md`.

## Data Flow

The generator writes weekday-only synthetic market data to S3:

```text
s3://<bucket>/raw/trades/dt=YYYY-MM-DD/hh=HH/part-<uuid>.jsonl.gz
s3://<bucket>/raw/quotes/dt=YYYY-MM-DD/hh=HH/part-<uuid>.jsonl.gz
s3://<bucket>/raw/events/events_YYYY-MM-DD.csv
```

The pipeline builds these Iceberg tables:

| Layer | Tables |
| --- | --- |
| Bronze | `bronze.trades_raw`, `bronze.quotes_raw`, `bronze.events_raw` |
| Silver | `silver.trades`, `silver.quotes`, `silver.events` |
| Gold | `gold.ohlcv_1m`, `gold.vwap_daily`, `gold.bid_ask_spread_1m`, `gold.event_windowed_volume` |

All timestamps and Airflow schedules use UTC.

## DAGs

| DAG | Schedule | Purpose |
| --- | --- | --- |
| `events_calendar_daily` | `30 5 * * 1-5` | Loads corporate events from raw CSV into Bronze and Silver |
| `trading_pipeline` | `*/15 6-22 * * 1-5` | Ingests trades and quotes every 15 minutes and updates intraday Gold tables |
| `gold_daily` | `0 23 * * 1-5` | Computes daily VWAP and event-windowed volume after market close |
| `iceberg_maintenance` | `0 2 * * 0` | Runs weekly Iceberg compaction, manifest rewrite, snapshot expiry, and orphan cleanup |

## Prerequisites

Install and configure these tools on Linux:

- AWS CLI v2
- Python 3.10 or newer
- `curl`
- Access to an AWS account with permissions for S3, IAM, Glue, EMR Serverless, MWAA, and Athena
- A VPC, private subnets, and security group suitable for MWAA
- IAM roles for EMR Serverless jobs and MWAA

Configure AWS credentials before running setup commands:

```bash
aws configure
```

Create a local environment file:

```bash
cat > setup.env <<'EOF'
export AWS_REGION=us-east-1
export BUCKET=<globally-unique-s3-bucket-name>
export EMR_JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/trading-emr-job-role
export MWAA_ROLE_ARN=arn:aws:iam::<account-id>:role/trading-mwaa-execution-role
export SUBNET_IDS=subnet-abc123,subnet-def456
export SG_IDS=sg-abc123
EOF

source setup.env
```

## IAM Roles

Create or reuse two roles:

| Role | Used by | Required access |
| --- | --- | --- |
| `trading-emr-job-role` | EMR Serverless jobs | S3 bucket access, Glue catalog access, CloudWatch/logging access |
| `trading-mwaa-execution-role` | MWAA | S3 bucket access, EMR Serverless job submission, Athena queries, Glue catalog access, CloudWatch/logging access |

For a production deployment, scope S3 permissions to the project bucket instead of using broad managed policies.

## AWS Setup

### 1. Create S3 Bucket and Prefixes

```bash
aws s3 mb "s3://$BUCKET" --region "$AWS_REGION"

for prefix in raw/trades raw/quotes raw/events warehouse airflow/dags jobs deequ athena-results logs; do
  aws s3api put-object --bucket "$BUCKET" --key "${prefix}/"
done
```

### 2. Upload Jobs and DAGs

```bash
aws s3 sync pyspark_jobs/ "s3://$BUCKET/jobs/" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"

aws s3 sync dags/ "s3://$BUCKET/airflow/dags/" \
  --exclude "__pycache__/*" \
  --exclude "*.pyc"
```

### 3. Stage PyDeequ Dependencies

The Silver jobs use PyDeequ, so stage both the Deequ JAR and a packed Python environment in S3.

```bash
curl -Lo deequ-2.0.7-spark-3.5.jar \
  https://repo1.maven.org/maven2/com/amazon/deequ/deequ/2.0.7-spark-3.5/deequ-2.0.7-spark-3.5.jar

aws s3 cp deequ-2.0.7-spark-3.5.jar "s3://$BUCKET/deequ/"
```

Build the PyDeequ archive on Amazon Linux 2, such as an EC2 instance, so the environment matches EMR Serverless:

```bash
python3 -m venv pydeequ_venv
source pydeequ_venv/bin/activate
pip install --upgrade pip
pip install pydeequ==1.4.0 venv-pack
venv-pack -o pydeequ-venv.tar.gz
deactivate

aws s3 cp pydeequ-venv.tar.gz "s3://$BUCKET/deequ/"
```

### 4. Create Glue Databases

```bash
for db in bronze silver gold; do
  aws glue create-database \
    --region "$AWS_REGION" \
    --database-input "Name=${db},Description=Trading lake ${db} layer,LocationUri=s3://${BUCKET}/warehouse/${db}.db/"
done
```

### 5. Create EMR Serverless Application

```bash
EMR_SERVERLESS_APP_ID=$(
  aws emr-serverless create-application \
    --region "$AWS_REGION" \
    --name trading-pipeline \
    --type SPARK \
    --release-label emr-7.1.0 \
    --query 'applicationId' \
    --output text
)

echo "export EMR_SERVERLESS_APP_ID=$EMR_SERVERLESS_APP_ID" >> setup.env
source setup.env
```

### 6. Create Iceberg Tables

Start the EMR Serverless application:

```bash
aws emr-serverless start-application \
  --region "$AWS_REGION" \
  --application-id "$EMR_SERVERLESS_APP_ID"
```

Submit the one-time table creation jobs:

```bash
submit_table_job() {
  local script="$1"

  aws emr-serverless start-job-run \
    --region "$AWS_REGION" \
    --application-id "$EMR_SERVERLESS_APP_ID" \
    --execution-role-arn "$EMR_JOB_ROLE_ARN" \
    --job-driver "sparkSubmit={entryPoint=s3://$BUCKET/jobs/${script},sparkSubmitParameters=\"--conf spark.jars=/usr/share/aws/iceberg/lib/iceberg-spark3-runtime.jar --conf spark.emr-serverless.driverEnv.BUCKET=$BUCKET --conf spark.executorEnv.BUCKET=$BUCKET\"}" \
    --configuration-overrides "monitoringConfiguration={s3MonitoringConfiguration={logUri=s3://$BUCKET/logs/}}" \
    --query 'jobRunId' \
    --output text
}

submit_table_job create_bronze_tables.py
submit_table_job create_silver_tables.py
submit_table_job create_gold_tables.py
```

Wait for each job run to finish successfully before continuing:

```bash
aws emr-serverless get-job-run \
  --region "$AWS_REGION" \
  --application-id "$EMR_SERVERLESS_APP_ID" \
  --job-run-id <job-run-id>
```

### 7. Create MWAA Environment

```bash
aws mwaa create-environment \
  --region "$AWS_REGION" \
  --name trading-mwaa \
  --airflow-version 2.10.5 \
  --source-bucket-arn "arn:aws:s3:::$BUCKET" \
  --dag-s3-path airflow/dags \
  --execution-role-arn "$MWAA_ROLE_ARN" \
  --environment-class mw1.small \
  --max-workers 5 \
  --network-configuration "SubnetIds=$SUBNET_IDS,SecurityGroupIds=$SG_IDS" \
  --airflow-configuration-options "core.default_timezone=UTC"
```

MWAA creation can take 20 to 30 minutes.

### 8. Configure Airflow Variables

In the MWAA Airflow UI, go to Admin -> Variables and create:

| Key | Value |
| --- | --- |
| `BUCKET` | `<your-bucket-name>` |
| `EMR_SERVERLESS_APP_ID` | `<your-emr-serverless-application-id>` |
| `EMR_JOB_ROLE_ARN` | `arn:aws:iam::<account-id>:role/trading-emr-job-role` |
| `ATHENA_OUTPUT` | `s3://<bucket>/athena-results/` |

## Generate Synthetic Data

Install local generator dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install boto3 numpy
```

Generate and upload raw data:

```bash
python generator/generate_synthetic.py \
  --bucket "$BUCKET" \
  --start-date 2024-01-15 \
  --end-date 2024-01-19
```

The generator writes only weekdays. It uploads trades, quotes, and events directly to S3.

## Run the Pipeline

After MWAA is ready and Airflow variables are configured:

1. Enable the DAGs in the Airflow UI.
2. Confirm that raw data exists under `raw/trades`, `raw/quotes`, and `raw/events`.
3. Trigger or backfill `events_calendar_daily` before `trading_pipeline` for the same processing date.
4. Trigger or backfill `trading_pipeline`.
5. Trigger `gold_daily` after the trading pipeline has completed for the day.

Example Airflow CLI backfill from an MWAA shell or compatible Airflow environment:

```bash
airflow dags backfill events_calendar_daily \
  --start-date 2024-01-15 \
  --end-date 2024-01-19

airflow dags backfill trading_pipeline \
  --start-date 2024-01-15 \
  --end-date 2024-01-19

airflow dags backfill gold_daily \
  --start-date 2024-01-15 \
  --end-date 2024-01-19
```
