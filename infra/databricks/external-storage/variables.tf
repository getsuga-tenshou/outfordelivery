variable "databricks_host" {
  type        = string
  description = "URL of the existing Databricks workspace, e.g. https://dbc-1234abcd-5678.cloud.databricks.com. Authenticate with DATABRICKS_TOKEN (or other DATABRICKS_* env vars)."
}

variable "lake_bucket" {
  type        = string
  description = "Name of the project S3 lake bucket (terraform -chdir=infra output lake_bucket). Unity Catalog gets the uc/ prefix only."
}

variable "aws_region" {
  type        = string
  default     = "eu-central-1"
  description = "AWS region of the lake bucket. Matches infra/variables.tf."
}

variable "aws_profile" {
  type        = string
  default     = null
  description = "AWS CLI named profile. Leave null to use the default credential chain. Same variable as infra/."
}

variable "aws_account_id" {
  type        = string
  description = "AWS account ID Terraform is allowed to run against (aws sts get-caller-identity). Same guard as infra/."
}

variable "project" {
  type        = string
  default     = "outfordelivery"
  description = "Name prefix for the IAM role and the Databricks objects."
}
