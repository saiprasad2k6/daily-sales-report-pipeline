variable "project_id" {
  description = "Existing Google Cloud project with billing enabled. Set via TF_VAR_project_id."
  type        = string
}

variable "region" {
  description = "Region supporting Composer 3, Cloud Storage, and BigQuery."
  type        = string
  default     = "us-central1"
}

variable "environment_name" {
  type    = string
  default = "daily-sales-composer"
}

variable "sales_bucket_name" {
  description = "Globally unique name for a new sales data bucket."
  type        = string
}

variable "composer_image_version" {
  description = "Explicit, region-supported Composer 3 image with Airflow 2."
  type        = string
  validation {
    condition     = can(regex("^composer-3-airflow-2\\.", var.composer_image_version))
    error_message = "Choose a Composer 3 image with Airflow 2 for the current DAG."
  }
}
