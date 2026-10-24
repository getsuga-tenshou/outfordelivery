variable "databricks_host" {
  type        = string
  description = "URL of the existing Databricks workspace, e.g. https://dbc-1234abcd-5678.cloud.databricks.com. Authenticate with DATABRICKS_TOKEN (or other DATABRICKS_* env vars)."
}

variable "project" {
  type        = string
  default     = "outfordelivery"
  description = "Name prefix for the IAM role and the Databricks objects."
}

variable "kafka_bootstrap" {
  default     = ""
  type        = string
  sensitive   = true
  description = "Kafka bootstrap address that Databricks can reach (host:port)."
}

variable "schema_registry_url" {
  default     = ""
  type        = string
  sensitive   = true
  description = "Schema Registry URL that Databricks can reach."
}

variable "secret_aws_access_key_id" {
  default     = ""
  type        = string
  sensitive   = true
  description = "Access key ID the Databricks jobs use for DynamoDB. Prefer an instance profile where the workspace supports one."
}

variable "secret_aws_secret_access_key" {
  default     = ""
  type        = string
  sensitive   = true
  description = "Secret access key paired with secret_aws_access_key_id."
}

variable "snowflake_account" {
  default     = ""
  type        = string
  sensitive   = true
  description = "Snowflake account identifier (org-account) that the pipeline's dbt task connects to."
}

variable "snowflake_private_key" {
  default     = ""
  type        = string
  sensitive   = true
  description = "PEM private key of the OFD_DBT Snowflake user (contents of ~/.snowflake/ofd_dbt.p8)."
}

variable "snowflake_private_key_passphrase" {
  type        = string
  default     = ""
  sensitive   = true
  description = "Passphrase of that key. Leave empty if the key is not encrypted."
}

variable "use_external_storage" {
  type        = bool
  default     = false
  description = "Use a location provisioned by the separate external-storage root; managed mode needs no AWS provider."
}

variable "default_storage_catalog" {
  type = object({
    storage_root = string
    properties   = optional(map(string), {})
  })
  default     = null
  description = "Metadata returned by catalogs.get after SQL-bootstrapping a Default Storage catalog. Preserve its Databricks-owned root/properties when importing; this does not provision external storage."
}

variable "enable_kafka_source" {
  type        = bool
  default     = false
  description = "Create Kafka and registry secrets."
}

variable "enable_dynamodb" {
  type        = bool
  default     = false
  description = "Create AWS writer secrets."
}

variable "enable_snowflake_link" {
  type        = bool
  default     = false
  description = "Create the external Iceberg reader principal/grants (compatible storage only)."
}

variable "enable_dbt" {
  type        = bool
  default     = false
  description = "Create Snowflake secrets for optional Databricks-side dbt; independent of the reader."
}

variable "external_location_url" {
  type        = string
  default     = ""
  description = "Optional S3 UC location output from the separately authenticated external-storage root."
}
