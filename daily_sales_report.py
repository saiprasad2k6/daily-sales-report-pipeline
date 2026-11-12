"""Daily sales pipeline for Google Cloud Composer."""

import csv
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.timezone import utc


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"Missing required environment variable: {name}. "
            "Set it in the Composer environment configuration before loading this DAG."
        )
    return value


PROJECT_ID = required_env("SALES_PROJECT_ID")
DATASET_ID = "sales_data"
TABLE_ID = "daily_sales_report"
SALES_GCS_BUCKET = required_env("SALES_GCS_BUCKET")

GCS_RAW_PREFIX = "data/raw/sales/"
GCS_PROCESSED_PREFIX = "data/processed/sales/"
RAW_COLUMNS = {
    "sale_date",
    "product_id",
    "product_name",
    "category",
    "order_id",
    "quantity",
    "unit_price",
}
PROCESSED_COLUMNS = [
    "sale_date",
    "product_id",
    "product_name",
    "category",
    "order_id",
    "quantity",
    "unit_price",
    "line_revenue",
]
AGGREGATED_COLUMNS = [
    "sale_date",
    "product_id",
    "product_name",
    "category",
    "total_orders",
    "total_quantity",
    "total_revenue",
]


def gcs_hook():
    return GCSHook()


def upload_csv(object_name, fieldnames, rows):
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    gcs_hook().upload(
        bucket_name=SALES_GCS_BUCKET,
        object_name=object_name,
        data=output.getvalue(),
        mime_type="text/csv",
    )


def read_csv(object_name):
    content = gcs_hook().download(
        bucket_name=SALES_GCS_BUCKET,
        object_name=object_name,
    ).decode("utf-8")
    return list(csv.DictReader(StringIO(content)))


def extract_sales_data(**context):
    execution_date = context["ds"]
    raw_file = f"{GCS_RAW_PREFIX}sales_{execution_date}.csv"
    if not gcs_hook().exists(
        bucket_name=SALES_GCS_BUCKET,
        object_name=raw_file,
    ):
        raise FileNotFoundError(
            f"Raw sales file was not found: gs://{SALES_GCS_BUCKET}/{raw_file}"
        )
    context["ti"].xcom_push(key="raw_file", value=raw_file)


def validate_data(**context):
    raw_file = context["ti"].xcom_pull(
        key="raw_file",
        task_ids="extract_sales_data",
    )
    rows = read_csv(raw_file)
    if not rows:
        raise ValueError(f"Raw sales file is empty: {raw_file}")

    missing_columns = RAW_COLUMNS - set(rows[0])
    if missing_columns:
        raise ValueError(
            f"Raw sales file is missing columns: {sorted(missing_columns)}"
        )

    for row_number, row in enumerate(rows, start=2):
        try:
            datetime.strptime(row["sale_date"], "%Y-%m-%d")
            quantity = int(row["quantity"])
            unit_price = Decimal(row["unit_price"])
        except (InvalidOperation, TypeError, ValueError) as error:
            raise ValueError(
                f"Invalid raw row {row_number} in {raw_file}: {error}"
            ) from error
        if not row["product_id"] or not row["order_id"] or quantity <= 0 or unit_price < 0:
            raise ValueError(f"Invalid required value in raw row {row_number}: {row}")

def transform_sales(**context):
    execution_date = context["ds"]
    raw_file = context["ti"].xcom_pull(
        key="raw_file",
        task_ids="extract_sales_data",
    )
    rows = read_csv(raw_file)
    processed_rows = []
    for row in rows:
        quantity = int(row["quantity"])
        unit_price = Decimal(row["unit_price"])
        processed_rows.append(
            {
                **row,
                "quantity": str(quantity),
                "unit_price": f"{unit_price:.2f}",
                "line_revenue": f"{quantity * unit_price:.2f}",
            }
        )

    processed_file = f"{GCS_PROCESSED_PREFIX}sales_{execution_date}_processed.csv"
    upload_csv(processed_file, PROCESSED_COLUMNS, processed_rows)
    context["ti"].xcom_push(key="processed_file", value=processed_file)


def aggregate_sales(**context):
    execution_date = context["ds"]
    processed_file = context["ti"].xcom_pull(
        key="processed_file",
        task_ids="transform_sales",
    )
    rows = read_csv(processed_file)
    grouped = {}
    order_ids = defaultdict(set)
    for row in rows:
        key = (
            row["sale_date"],
            row["product_id"],
            row["product_name"],
            row["category"],
        )
        if key not in grouped:
            grouped[key] = {"total_quantity": 0, "total_revenue": Decimal("0")}
        grouped[key]["total_quantity"] += int(row["quantity"])
        grouped[key]["total_revenue"] += Decimal(row["line_revenue"])
        order_ids[key].add(row["order_id"])

    aggregated_rows = []
    for (sale_date, product_id, product_name, category), totals in grouped.items():
        aggregated_rows.append(
            {
                "sale_date": sale_date,
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "total_orders": len(order_ids[(sale_date, product_id, product_name, category)]),
                "total_quantity": totals["total_quantity"],
                "total_revenue": f"{totals['total_revenue']:.2f}",
            }
        )

    aggregated_file = f"{GCS_PROCESSED_PREFIX}sales_{execution_date}_aggregated.csv"
    upload_csv(aggregated_file, AGGREGATED_COLUMNS, aggregated_rows)
    context["ti"].xcom_push(key="aggregated_file", value=aggregated_file)


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="daily_sales_report",
    description="Daily sales pipeline: GCS raw -> processed -> aggregated -> BigQuery",
    default_args=default_args,
    start_date=datetime(2024, 1, 1, tzinfo=utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "bigquery"],
) as dag:
    extract_task = PythonOperator(
        task_id="extract_sales_data",
        python_callable=extract_sales_data,
    )
    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )
    transform_task = PythonOperator(
        task_id="transform_sales",
        python_callable=transform_sales,
    )
    aggregate_task = PythonOperator(
        task_id="aggregate_sales",
        python_callable=aggregate_sales,
    )
    load_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=SALES_GCS_BUCKET,
        source_objects=[
            "{{ ti.xcom_pull(task_ids='aggregate_sales', key='aggregated_file') }}"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
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
    def notify_success():
        print("[notify_success] Pipeline completed successfully.")

    notify_task = PythonOperator(task_id="notify_success", python_callable=notify_success)

    extract_task >> validate_task >> transform_task >> aggregate_task >> load_task >> notify_task
