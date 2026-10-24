mock_provider "snowflake" {}
mock_provider "snowflake" { alias = "bootstrap" }

variables {
  dbt_public_key       = "test-dbt-public-key"
  dashboard_public_key = "test-dashboard-public-key"
  loader_public_key    = "test-loader-public-key"
}

run "native_without_uc_credentials" {
  command = plan
  assert {
    condition     = length(snowflake_catalog_integration_iceberg_rest.uc) == 0 && length(snowflake_iceberg_table_from_rest.silver) == 0
    error_message = "Native sources must not require or create UC/Iceberg integrations."
  }
  assert {
    condition     = length(snowflake_table.native) == 6 && length(snowflake_service_user.loader) == 1 && length(snowflake_stage_internal.exports) == 1
    error_message = "Native mode must provision two published tables, two loading tables, audit, singleton lock and internal stage/loader."
  }
  assert {
    condition     = snowflake_service_user.loader[0].default_role == "OFD_LOADER" && snowflake_warehouse.ofd.auto_suspend == 60
    error_message = "Transfers must retain dedicated loader permissions and warehouse cost controls."
  }
}

run "compatible_storage_iceberg" {
  command = plan
  variables {
    source_mode            = "iceberg"
    databricks_host        = "https://example.cloud.databricks.com"
    uc_oauth_client_id     = "test-client"
    uc_oauth_client_secret = "test-secret"
    loader_public_key      = ""
  }
  assert {
    condition     = length(snowflake_catalog_integration_iceberg_rest.uc) == 1 && length(snowflake_iceberg_table_from_rest.silver) == 2 && length(snowflake_table.native) == 0 && length(snowflake_service_user.loader) == 0
    error_message = "Compatible mode must retain the original integration without native objects."
  }
}

run "reject_missing_iceberg_credentials" {
  command = plan
  variables { source_mode = "iceberg" }
  expect_failures = [snowflake_catalog_integration_iceberg_rest.uc]
}

run "reject_missing_loader_key" {
  command = plan
  variables { loader_public_key = "" }
  expect_failures = [snowflake_service_user.loader]
}
