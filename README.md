# Daily Sales Report Pipeline

This project contains an Apache Airflow DAG for Google Cloud Composer. The
DAG checks for a daily sales CSV in Cloud Storage and loads an aggregated CSV
into BigQuery.

## Project files

```text
.
├── daily_sales_report.py
├── requirements.txt
└── README.md
```

The DAG and its configuration file are expected in the same directory. There
is no `resources/` directory in this project.

## Prerequisites

- A Google Cloud project with Cloud Composer, Cloud Storage, and BigQuery
  enabled
- A Cloud Composer environment running a compatible version of Apache Airflow
- A Cloud Storage bucket
- A BigQuery dataset named `sales_data`
- A Composer service account that can read the bucket and write to BigQuery

The DAG writes to the BigQuery table
`PROJECT_ID.sales_data.daily_sales_report`. Create the dataset and table
before running the DAG, or allow the load operator to create the table from
the declared schema.

## Configuration

Create a `.env` file next to `daily_sales_report.py`:

```dotenv
PROJECT_ID=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
```

`GCS_BUCKET` must contain only the bucket name, without the `gs://` prefix.
Keep `.env` out of source control and do not put credentials in it.

The DAG expects files using these Cloud Storage paths, where
`YYYY-MM-DD` is the Airflow execution date:

```text
gs://YOUR_BUCKET/raw/sales/sales_YYYY-MM-DD.csv
gs://YOUR_BUCKET/processed/sales/sales_YYYY-MM-DD_aggregated.csv
```

The aggregated CSV must contain this header:

```text
sale_date,product_id,product_name,category,total_orders,total_quantity,total_revenue
```

## Install dependencies

Cloud Composer already provides Apache Airflow. Install the dependencies in
[`requirements.txt`](./requirements.txt) in the Composer environment:

1. Open **Composer > Environments > your environment > Python packages**.
2. Add the packages and versions from `requirements.txt`.
3. Apply the change and wait for the environment update to complete.

The repository-level `requirements.txt` is intended for the Composer
environment; it is not a replacement for Composer's built-in Airflow
installation.

## Deploy and run the DAG

Upload both files to the Composer environment's `dags/` directory:

```bash
gcloud composer environments storage dags import \
  --environment YOUR_COMPOSER_ENVIRONMENT \
  --location YOUR_COMPOSER_REGION \
  --source "daily_sales_report.py"

gcloud composer environments storage dags import \
  --environment YOUR_COMPOSER_ENVIRONMENT \
  --location YOUR_COMPOSER_REGION \
  --source ".env"
```

Then:

1. Open the Airflow UI from the Composer environment.
2. Find the `daily_sales_report` DAG and enable it.
3. Trigger a manual run for testing, or wait for its daily schedule.
4. Review the task logs and verify the rows in
   `sales_data.daily_sales_report`.

The DAG runs daily, does not catch up on missed dates, and permits only one
active run at a time.

## Current pipeline behavior

The extract, validation, transform, and aggregation tasks currently construct
and pass the expected Cloud Storage paths through XCom. They do not download,
transform, aggregate, or upload files. The expected aggregated CSV must
already exist at the processed path before `load_to_bigquery` runs, unless
those task bodies are implemented to produce it.

## Local syntax check

If Airflow and the listed dependencies are installed locally, check the DAG
syntax with:

```bash
python -m py_compile daily_sales_report.py
```

This checks Python syntax only; it does not connect to Google Cloud or execute
the DAG.
