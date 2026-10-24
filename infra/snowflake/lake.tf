# Snowflake reads the Databricks Silver tables in place: Unity Catalog serves their UniForm Iceberg metadata over
# its Iceberg REST endpoint and vends short-lived storage credentials, so no Snowflake external volume or AWS role is needed.
locals {
  uc_namespace = "silver"
  lake_tables = {
    SILVER_PARCEL_EVENTS = "parcel_events"
    SILVER_PARCELS       = "parcels"
  }
}

resource "snowflake_catalog_integration_iceberg_rest" "uc" {
  count                    = var.source_mode == "iceberg" ? 1 : 0
  name                     = "OFD_UC"
  enabled                  = true
  catalog_namespace        = local.uc_namespace
  refresh_interval_seconds = 300

  lifecycle {
    precondition {
      condition     = var.databricks_host != "" && var.uc_oauth_client_id != "" && var.uc_oauth_client_secret != ""
      error_message = "Iceberg mode requires a compatible UC host and OAuth credentials."
    }
  }

  rest_config {
    catalog_uri            = "${trimsuffix(var.databricks_host, "/")}/api/2.1/unity-catalog/iceberg-rest"
    catalog_name           = var.uc_catalog_name
    access_delegation_mode = "VENDED_CREDENTIALS"
  }

  oauth_rest_authentication {
    oauth_token_uri      = "${trimsuffix(var.databricks_host, "/")}/oidc/v1/token"
    oauth_client_id      = var.uc_oauth_client_id
    oauth_client_secret  = var.uc_oauth_client_secret
    oauth_allowed_scopes = ["all-apis"]
  }
}

resource "snowflake_iceberg_table_from_rest" "silver" {
  for_each           = var.source_mode == "iceberg" ? local.lake_tables : {}
  database           = snowflake_database.ofd.name
  schema             = snowflake_schema.lake.name
  name               = each.key
  catalog            = snowflake_catalog_integration_iceberg_rest.uc[0].name
  catalog_table_name = each.value
  catalog_namespace  = local.uc_namespace
  auto_refresh       = "true"
}

moved {
  from = snowflake_catalog_integration_iceberg_rest.uc
  to   = snowflake_catalog_integration_iceberg_rest.uc[0]
}
