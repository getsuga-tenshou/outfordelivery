mock_provider "databricks" {}

variables {
  databricks_host         = "https://example.cloud.databricks.com"
  default_storage_catalog = null
}

# Managed configuration has no AWS provider; run with all AWS credentials absent.
run "managed_without_aws_authentication" {
  command = plan
  assert {
    condition     = databricks_catalog.ofd.storage_root == null
    error_message = "Managed mode must omit external storage and the catalog storage root."
  }
  assert {
    condition     = length(databricks_secret_scope.ofd) == 0 && length(databricks_service_principal.snowflake) == 0
    error_message = "Managed defaults must not create integration secrets or principals."
  }
}

run "compatible_workspace" {
  command = plan
  variables {
    use_external_storage         = true
    external_location_url        = "s3://ofd-test-lake/uc"
    enable_kafka_source          = true
    kafka_bootstrap              = "broker.example.com:9092"
    schema_registry_url          = "https://registry.example.com"
    enable_dynamodb              = true
    secret_aws_access_key_id     = "test-placeholder"
    secret_aws_secret_access_key = "test-placeholder"
    enable_snowflake_link        = true
    enable_dbt                   = true
    snowflake_account            = "test-account"
    snowflake_private_key        = "test-placeholder"
  }
  assert {
    condition     = databricks_catalog.ofd.storage_root == "s3://ofd-test-lake/uc/outfordelivery" && length(databricks_secret_scope.ofd) == 1 && length(databricks_service_principal.snowflake) == 1
    error_message = "Compatible mode must retain external storage and enabled integrations."
  }
}

run "bootstrapped_default_storage_catalog" {
  command = plan
  variables {
    default_storage_catalog = {
      storage_root = "s3://databricks-owned-default/uc/metastore"
      properties   = { collation = "UTF8_BINARY" }
    }
  }
  assert {
    condition     = databricks_catalog.ofd.storage_root == "s3://databricks-owned-default/uc/metastore" && databricks_catalog.ofd.properties["collation"] == "UTF8_BINARY"
    error_message = "Imported Default Storage catalog metadata must be retained to prevent replacement or property removal."
  }
  assert {
    condition     = length(databricks_secret_scope.ofd) == 0 && length(databricks_service_principal.snowflake) == 0
    error_message = "Adopting Default Storage must not enable external integrations."
  }
}

run "dbt_without_iceberg_reader" {
  command = plan
  variables {
    enable_dbt            = true
    snowflake_account     = "test-account"
    snowflake_private_key = "test-placeholder"
  }
  assert {
    condition     = length(databricks_secret.snowflake_account) == 1 && length(databricks_service_principal.snowflake) == 0
    error_message = "dbt secrets and the external Iceberg reader must be independently gated."
  }
}

run "reject_default_storage_iceberg_link" {
  command = plan
  variables { enable_snowflake_link = true }
  expect_failures = [databricks_catalog.ofd]
}
