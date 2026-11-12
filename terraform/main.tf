terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "apis" {
  for_each = toset([
    "composer.googleapis.com",
    "compute.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
  ])
  service            = each.value
  disable_on_destroy = false
}

resource "google_service_account" "composer" {
  account_id   = "daily-sales-composer"
  display_name = "Daily sales Composer environment"
  depends_on   = [google_project_service.apis]
}

resource "google_project_iam_member" "composer_roles" {
  for_each = toset(["roles/composer.worker", "roles/bigquery.jobUser"])
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_storage_bucket" "sales" {
  name                        = var.sales_bucket_name
  location                    = var.region
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  depends_on                  = [google_project_service.apis]
}

resource "google_storage_bucket_iam_member" "sales_reader" {
  bucket = google_storage_bucket.sales.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_bigquery_dataset" "sales" {
  dataset_id                 = "sales_data"
  location                   = var.region
  delete_contents_on_destroy = false
  depends_on                 = [google_project_service.apis]
}

resource "google_bigquery_dataset_iam_member" "sales_writer" {
  dataset_id = google_bigquery_dataset.sales.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_bigquery_table" "report" {
  dataset_id          = google_bigquery_dataset.sales.dataset_id
  table_id            = "daily_sales_report"
  deletion_protection = false
  schema = jsonencode([
    { name = "sale_date", type = "DATE", mode = "REQUIRED" },
    { name = "product_id", type = "STRING", mode = "REQUIRED" },
    { name = "product_name", type = "STRING", mode = "NULLABLE" },
    { name = "category", type = "STRING", mode = "NULLABLE" },
    { name = "total_orders", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_quantity", type = "INTEGER", mode = "REQUIRED" },
    { name = "total_revenue", type = "FLOAT", mode = "REQUIRED" },
  ])
}

resource "google_composer_environment" "sales" {
  name   = var.environment_name
  region = var.region

  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"
    node_config {
      service_account = google_service_account.composer.email
    }
    software_config {
      image_version = var.composer_image_version
      env_variables = {
        SALES_PROJECT_ID = var.project_id
        SALES_GCS_BUCKET = google_storage_bucket.sales.name
      }
      # Airflow and the Google provider are bundled with the Composer image.
    }
  }

  depends_on = [
    google_project_service.apis,
    google_project_iam_member.composer_roles,
    google_storage_bucket_iam_member.sales_reader,
    google_bigquery_dataset_iam_member.sales_writer,
    google_bigquery_table.report,
  ]
}

# Composer supplies a prefix like gs://managed-environment-bucket/dags.
resource "google_storage_bucket_object" "sales_dag" {
  bucket = split("/", trimprefix(google_composer_environment.sales.config[0].dag_gcs_prefix, "gs://"))[0]
  name = "${join("/", slice(
    split("/", trimprefix(google_composer_environment.sales.config[0].dag_gcs_prefix, "gs://")),
    1,
    length(split("/", trimprefix(google_composer_environment.sales.config[0].dag_gcs_prefix, "gs://")))
  ))}/daily_sales_report.py"
  source         = "${path.module}/../daily_sales_report.py"
  detect_md5hash = filemd5("${path.module}/../daily_sales_report.py")
}
