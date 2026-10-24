terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.139" }
  }
}

provider "databricks" {
  host = var.databricks_host
}

locals {
  needs_secrets = var.enable_kafka_source || var.enable_dbt || (var.enable_dynamodb && nonsensitive(var.secret_aws_access_key_id != ""))
}

resource "databricks_catalog" "ofd" {
  name         = var.project
  storage_root = var.use_external_storage ? "${var.external_location_url}/${var.project}" : try(var.default_storage_catalog.storage_root, null)
  properties   = try(var.default_storage_catalog.properties, null)
  comment      = "Managed by Terraform (infra/databricks)"
  lifecycle {
    precondition {
      condition     = !var.use_external_storage || var.default_storage_catalog == null
      error_message = "Default Storage import metadata cannot be combined with customer external storage."
    }
    precondition {
      condition     = !var.use_external_storage || startswith(var.external_location_url, "s3://")
      error_message = "External mode requires a reviewed external_location_url from the external-storage root."
    }
    precondition {
      condition     = !var.enable_snowflake_link || var.use_external_storage
      error_message = "External Iceberg readers require compatible external storage, not default storage."
    }
    precondition {
      condition     = !var.enable_kafka_source || (var.kafka_bootstrap != "" && var.schema_registry_url != "")
      error_message = "Kafka mode requires bootstrap and Schema Registry secrets."
    }
    precondition {
      condition     = !var.enable_dbt || (var.snowflake_account != "" && var.snowflake_private_key != "")
      error_message = "Databricks-side dbt requires Snowflake account and private key."
    }
    precondition {
      condition     = !var.enable_dynamodb || ((var.secret_aws_access_key_id == "") == (var.secret_aws_secret_access_key == ""))
      error_message = "DynamoDB static keys must be supplied as a pair, or both omitted for an instance profile."
    }
  }
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.ofd.name
  name         = "bronze"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.ofd.name
  name         = "silver"
}

# Checkpoints and volumes
resource "databricks_schema" "ops" {
  catalog_name = databricks_catalog.ofd.name
  name         = "ops"
}

resource "databricks_volume" "raw" {
  catalog_name = databricks_catalog.ofd.name
  schema_name  = databricks_schema.ops.name
  name         = "raw"
  volume_type  = "MANAGED"
  comment      = "Completed paired input batches and compatible legacy drops"
}

# Structured Streaming checkpoints
resource "databricks_volume" "checkpoints" {
  catalog_name = databricks_catalog.ofd.name
  schema_name  = databricks_schema.ops.name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Streaming checkpoints"
}

resource "databricks_volume" "exports" {
  catalog_name = databricks_catalog.ofd.name
  schema_name  = databricks_schema.ops.name
  name         = "exports"
  volume_type  = "MANAGED"
  comment      = "Completed logical Silver snapshots retained for device download"
}

resource "databricks_secret_scope" "ofd" {
  count = local.needs_secrets ? 1 : 0
  name  = var.project
}

# An instance profile is preferred over static AWS keys where the workspace supports one.
resource "databricks_secret" "kafka_bootstrap" {
  count        = var.enable_kafka_source ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "kafka_bootstrap"
  string_value = var.kafka_bootstrap
}

resource "databricks_secret" "schema_registry_url" {
  count        = var.enable_kafka_source ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "schema_registry_url"
  string_value = var.schema_registry_url
}

resource "databricks_secret" "aws_access_key_id" {
  count        = var.enable_dynamodb && nonsensitive(var.secret_aws_access_key_id != "") ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "aws_access_key_id"
  string_value = var.secret_aws_access_key_id
}

resource "databricks_secret" "aws_secret_access_key" {
  count        = var.enable_dynamodb && nonsensitive(var.secret_aws_secret_access_key != "") ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "aws_secret_access_key"
  string_value = var.secret_aws_secret_access_key
}

# Snowflake reads Silver through Unity Catalog's Iceberg REST endpoint with this service principal (ticket 09).
resource "databricks_service_principal" "snowflake" {
  count        = var.enable_snowflake_link ? 1 : 0
  display_name = "${var.project}-snowflake-reader"
}

resource "databricks_service_principal_secret" "snowflake" {
  count                = var.enable_snowflake_link ? 1 : 0
  service_principal_id = databricks_service_principal.snowflake[0].id
}

resource "databricks_grant" "snowflake_catalog" {
  count      = var.enable_snowflake_link ? 1 : 0
  catalog    = databricks_catalog.ofd.name
  principal  = databricks_service_principal.snowflake[0].application_id
  privileges = ["USE_CATALOG"]
}

# EXTERNAL_USE_SCHEMA is what lets an external engine receive vended storage credentials for this schema.
resource "databricks_grant" "snowflake_silver" {
  count      = var.enable_snowflake_link ? 1 : 0
  schema     = databricks_schema.silver.id
  principal  = databricks_service_principal.snowflake[0].application_id
  privileges = ["USE_SCHEMA", "SELECT", "EXTERNAL_USE_SCHEMA"]
}

# dbt runs from the pipeline job and connects to Snowflake as OFD_DBT (ticket 10).
resource "databricks_secret" "snowflake_account" {
  count        = var.enable_dbt ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "snowflake_account"
  string_value = var.snowflake_account
}

resource "databricks_secret" "snowflake_private_key" {
  count        = var.enable_dbt ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "snowflake_private_key"
  string_value = var.snowflake_private_key
}

resource "databricks_secret" "snowflake_private_key_passphrase" {
  count        = var.enable_dbt && nonsensitive(var.snowflake_private_key_passphrase != "") ? 1 : 0
  scope        = databricks_secret_scope.ofd[0].id
  key          = "snowflake_private_key_passphrase"
  string_value = var.snowflake_private_key_passphrase
}

# Preserve existing integration resource addresses when explicitly re-enabled.
moved {
  from = databricks_secret_scope.ofd
  to   = databricks_secret_scope.ofd[0]
}
moved {
  from = databricks_secret.kafka_bootstrap
  to   = databricks_secret.kafka_bootstrap[0]
}
moved {
  from = databricks_secret.schema_registry_url
  to   = databricks_secret.schema_registry_url[0]
}
moved {
  from = databricks_secret.aws_access_key_id
  to   = databricks_secret.aws_access_key_id[0]
}
moved {
  from = databricks_secret.aws_secret_access_key
  to   = databricks_secret.aws_secret_access_key[0]
}
moved {
  from = databricks_secret.snowflake_account
  to   = databricks_secret.snowflake_account[0]
}
moved {
  from = databricks_secret.snowflake_private_key
  to   = databricks_secret.snowflake_private_key[0]
}
moved {
  from = databricks_service_principal.snowflake
  to   = databricks_service_principal.snowflake[0]
}
moved {
  from = databricks_service_principal_secret.snowflake
  to   = databricks_service_principal_secret.snowflake[0]
}
moved {
  from = databricks_grant.snowflake_catalog
  to   = databricks_grant.snowflake_catalog[0]
}
moved {
  from = databricks_grant.snowflake_silver
  to   = databricks_grant.snowflake_silver[0]
}
