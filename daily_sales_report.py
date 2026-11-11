# Apache Airflow DAG for the Google Cloud Composer End-to-End Demo.
# Pipeline:
# extract_sales_data -> validate_data -> transform_sales -> aggregate_sales -> load_to_bigquery -> notify_success

from datetime import datetime, timedelta
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

#
# Configuration
#

load_dotenv(Path(__file__).with_name(".env"))


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it in .env next to this DAG before loading it."
        )
    return value

# Dataset and table must be pre-created in BigQuery
PROJECT_ID = required_env("PROJECT_ID")
DATASET_ID = "sales_data"
TABLE_ID = "daily_sales_report"

GCS_BUCKET = required_env("GCS_BUCKET")

GCS_RAW_PREFIX = "raw/sales/"
GCS_PROCESSED_PREFIX = "processed/sales/"

#
# Default arguments
#
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

#
# DAG definition
#
with DAG(
    dag_id="daily_sales_report",
    description="End-to-end daily sales pipeline: GCS -> transform -> BigQuery",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "bigquery", "demo"],
) as dag:

    def extract_sales_data(**context):
        execution_date = context["ds"]
        print(f"[extract_sales_data] Checking for raw sales files for {execution_date}")
        expected_file = f"{GCS_RAW_PREFIX}sales_{execution_date}.csv"
        context["ti"].xcom_push(key="raw_file", value=expected_file)

    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data,
    )

    def validate_data(**context):
        raw_file = context["ti"].xcom_pull(key="raw_file", task_ids="extract_sales_data")
        print(f"[validate_data] Validating: gs://{GCS_BUCKET}/{raw_file}")

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    def transform_sales(**context):
        execution_date = context["ds"]
        processed_file = f"{GCS_PROCESSED_PREFIX}sales_{execution_date}_processed.csv"
        context["ti"].xcom_push(key="processed_file", value=processed_file)

    transform_task = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales,
    )

    def aggregate_sales(**context):
        execution_date = context["ds"]
        aggregated_file = f"{GCS_PROCESSED_PREFIX}sales_{execution_date}_aggregated.csv"
        context["ti"].xcom_push(key="aggregated_file", value=aggregated_file)

    aggregate_task = PythonOperator(
        task_id="aggregate_sales",
        python_callable=aggregate_sales,
    )

    load_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull(task_ids='aggregate_sales', key='aggregated_file') }}"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        schema_fields=[
            {"name": "sale_date", "type": "DATE", "mode": "REQUIRED"},
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "total_orders", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "total_quantity", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "total_revenue", "type": "FLOAT", "mode": "REQUIRED"},
        ],
    )

    def notify_success(**context):
        print("[notify_success] Pipeline completed successfully.")

    notify_task = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
    )

    extract_task >> validate_task >> transform_task >> aggregate_task >> load_task >> notify_task
