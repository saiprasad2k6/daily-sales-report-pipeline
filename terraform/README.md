# Provision the sales pipeline with Terraform

This configuration creates a small Composer 3 environment running Airflow 2,
a dedicated service account, a sales data bucket, and the `sales_data` BigQuery
dataset and `daily_sales_report` table. It enables the required APIs and grants
the environment permission to read sales objects and run BigQuery load jobs.
The bucket and dataset use the same region.

Composer is a paid, continuously running managed service. Review
[Composer pricing](https://cloud.google.com/composer/pricing) before applying.
This example uses the default public connectivity configuration; organization
policies may require additional network configuration.

## 1. Install tools and prepare your project

Use an existing Google Cloud project with billing enabled. For a first demo,
use a dedicated project without an existing `sales_data` dataset or the
resource names in this example. Existing resources must be imported into
Terraform rather than created again.

Install [Terraform](https://developer.hashicorp.com/terraform/install)
(version 1.5 or newer) and the
[Google Cloud CLI](https://cloud.google.com/sdk/docs/install).
On macOS with Homebrew:

```bash
brew tap hashicorp/tap
brew install hashicorp/tap/terraform
brew install --cask gcloud-cli
terraform version
gcloud version
```

Terraform and gcloud do not require a Python virtual environment.

Your deploying identity needs permissions to enable APIs, create Composer
environments, create service accounts, act as the environment's service
account, and manage project, bucket, and dataset IAM and resources. An
administrator can grant the relevant roles: Service Usage Admin, Composer
Admin, Service Account Admin, Service Account User, Project IAM Admin,
Storage Admin, and BigQuery Admin. Organization policies can restrict these
operations. Do not grant these deployment roles to the runtime account;
Terraform assigns its narrower permissions.

Authenticate both the CLI and Terraform, replacing the project ID:

```bash
export PROJECT_ID="your-gcp-project-id"
export TF_VAR_project_id="$PROJECT_ID"
gcloud auth login
gcloud config set project "$PROJECT_ID"
gcloud auth application-default login
gcloud auth application-default set-quota-project "$PROJECT_ID"
```

## 2. Configure Terraform

From the repository root:

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars
```

Terraform reads the project ID from `TF_VAR_project_id` in your shell.
Export it again when opening a new terminal. Keep `project_id` out of
`terraform.tfvars`: values in that file override environment variables.

Edit `terraform.tfvars` with a supported region and a globally unique bucket
name. If the file already exists, edit it directly instead of copying over it.
The example image version is illustrative;
check the available images in the Google Cloud console's Composer **Create
environment** page for your selected region before using it. Choose an
explicit **Composer 3 / Airflow 2** version. This DAG uses Airflow 2 APIs;
an Airflow 3 image requires DAG migration. Explicit versions avoid an
unexpected upgrade when applying later.

The Terraform code is in:

- [main.tf](./main.tf): providers, APIs, IAM, storage, BigQuery, and Composer.
- [variables.tf](./variables.tf): configurable inputs and image validation.
- [outputs.tf](./outputs.tf): environment name, region, Airflow URL, and buckets.
- [terraform.tfvars.example](./terraform.tfvars.example): sample input values.

Composer supplies Airflow and its Google provider. Composer receives
`SALES_PROJECT_ID` and `SALES_GCS_BUCKET`
through `software_config.env_variables`, which the current DAG already reads.

## 3. Review and create infrastructure

Run these commands from `terraform/`:

```bash
terraform init
terraform fmt -check
terraform validate
terraform plan -out=deploy.tfplan
terraform apply deploy.tfplan
terraform output
```

Review the plan before the apply command, which creates billable resources.
Composer creation can take several minutes. If IAM permissions have not
propagated, wait briefly and run a new plan and apply again.

Keep the generated `.terraform.lock.hcl` in source control. Local state,
variable values, and plan files are ignored by Git. Keep the state safe:
future Terraform commands use it to track the infrastructure. For team
use, configure a GCS backend in a separate existing state bucket before
the first apply; do not use the sales data bucket for state.

## 4. Upload and run the DAG

Terraform automatically uploads `daily_sales_report.py` to Composer's managed
`dags/` prefix after the environment is created. The `sales_dag` storage object
tracks the file's checksum, so subsequent applies upload changed Python code.
After editing the DAG, run from `terraform/`:

```bash
terraform plan -out=deploy.tfplan
terraform apply deploy.tfplan
```

No `.env` upload is needed for this setup. The identity running Terraform
needs object write access to Composer's managed bucket. Terraform
uploads the DAG on apply; it does not watch for local file changes continuously.
Destroying this configuration also removes the managed DAG object.

Open the URL printed by `terraform output -raw airflow_uri`. Wait for
`daily_sales_report` to appear, check for import errors, then enable it.
Before triggering a run, upload an aggregated CSV for the intended run's
Airflow logical date. For example, if that date is `2026-11-11`:

```bash
cat > /tmp/sales_2026-11-11_aggregated.csv <<'CSV'
sale_date,product_id,product_name,category,total_orders,total_quantity,total_revenue
2026-11-11,P001,Notebook,Stationery,2,3,150.0
CSV

gcloud storage cp /tmp/sales_2026-11-11_aggregated.csv \
  "gs://$(terraform output -raw sales_bucket_name)/processed/sales/sales_2026-11-11_aggregated.csv"
```

Use the logical date shown in the Airflow UI for your actual run, replacing
the example date in the filename, destination path, and CSV row. Trigger the
DAG and inspect task logs and the BigQuery table. Every successful load
replaces the table contents (`WRITE_TRUNCATE`).

The DAG validates raw CSV files, writes processed and aggregated CSV files to
the sales bucket, and loads the aggregated file into BigQuery. The runtime
service account therefore needs both read and write access to sales objects;
the Terraform configuration grants `roles/storage.objectAdmin` on the bucket.

## 5. Clean up after the demo

From `terraform/`, review what will be deleted:

```bash
terraform plan -destroy
```

The report table has `deletion_protection = true`. To intentionally delete
it, change that setting to `false` in `main.tf`, review and apply that
change, then run:

```bash
terraform destroy
```

The sales bucket refuses deletion while it contains objects. Delete or
archive those objects intentionally before retrying cleanup. The dataset
also refuses deletion if unmanaged tables remain. Composer's managed bucket
can remain after environment deletion; inspect and remove it separately
when its logs and DAG files are no longer needed. Enabled APIs remain
enabled. Confirm environment deletion in the console to stop its running cost.

References: [Google's Terraform setup guide](https://docs.cloud.google.com/composer/docs/composer-3/terraform-create-environments),
[Python dependency configuration](https://docs.cloud.google.com/composer/docs/composer-3/install-python-dependencies),
and [Terraform Composer resource](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/composer_environment).
