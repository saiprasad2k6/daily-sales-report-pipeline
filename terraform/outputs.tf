output "environment_name" {
  value = google_composer_environment.sales.name
}

output "region" {
  value = var.region
}

output "airflow_uri" {
  value = google_composer_environment.sales.config[0].airflow_uri
}

output "dag_gcs_prefix" {
  value = google_composer_environment.sales.config[0].dag_gcs_prefix
}

output "sales_bucket_name" {
  value = google_storage_bucket.sales.name
}
