terraform {
  required_version = ">= 1.5"
  required_providers {
    snowflake = { source = "snowflakedb/snowflake", version = "~> 2.22" }
  }
}

provider "snowflake" {
  preview_features_enabled = ["snowflake_table_resource"]
  organization_name        = var.snowflake_organization_name
  account_name             = var.snowflake_account_name
  user                     = var.snowflake_admin_user
  password                 = var.snowflake_admin_password
  role                     = var.snowflake_admin_role
}

# Only the singleton seed INSERT needs compute. Use the project warehouse after its creation,
# rather than inheriting (and possibly resuming) the administrator's default warehouse.
# The computed ID defers warehouse selection until creation; its name is known too early during a fresh plan.
provider "snowflake" {
  alias             = "bootstrap"
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_admin_user
  password          = var.snowflake_admin_password
  role              = var.snowflake_admin_role
  warehouse         = replace(snowflake_warehouse.ofd.id, "\"", "")
}

resource "snowflake_resource_monitor" "ofd" {
  name                      = "OFD_MONITOR"
  credit_quota              = var.monthly_credit_quota
  frequency                 = "MONTHLY"
  start_timestamp           = "IMMEDIATELY"
  notify_triggers           = [80]
  suspend_trigger           = 100
  suspend_immediate_trigger = 110
}

resource "snowflake_warehouse" "ofd" {
  name                = "OFD_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  resource_monitor    = snowflake_resource_monitor.ofd.fully_qualified_name
}

resource "snowflake_database" "ofd" {
  name = "OUTFORDELIVERY"
}

# Published Silver sources and native loading/audit objects.
resource "snowflake_schema" "lake" {
  database = snowflake_database.ofd.name
  name     = "LAKE"
}

resource "snowflake_schema" "staging" {
  database = snowflake_database.ofd.name
  name     = "STAGING"
}

resource "snowflake_schema" "marts" {
  database = snowflake_database.ofd.name
  name     = "MARTS"
}

resource "snowflake_account_role" "transformer" {
  name = "OFD_TRANSFORMER"
}

resource "snowflake_account_role" "reader" {
  name = "OFD_READER"
}

resource "snowflake_grant_privileges_to_account_role" "transformer_warehouse" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ofd.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_database" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.ofd.name
  }
}

# dbt creates and owns the objects in STAGING and MARTS.
resource "snowflake_grant_privileges_to_account_role" "transformer_build" {
  for_each          = { staging = snowflake_schema.staging, marts = snowflake_schema.marts }
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE", "CREATE TABLE", "CREATE VIEW"]
  on_schema {
    schema_name = each.value.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_lake_schema" {
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.lake.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "transformer_lake_select" {
  # Native sources have explicit grants; future grants would also expose loader internals.
  for_each          = var.source_mode == "iceberg" ? toset(["TABLES", "ICEBERG TABLES"]) : toset([])
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = each.value
      in_schema          = snowflake_schema.lake.fully_qualified_name
    }
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_warehouse" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.ofd.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_database" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.ofd.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "reader_marts_schema" {
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.marts.fully_qualified_name
  }
}

# Tables and views that dbt creates in MARTS after this grant.
resource "snowflake_grant_privileges_to_account_role" "reader_marts_select" {
  for_each          = toset(["TABLES", "VIEWS"])
  account_role_name = snowflake_account_role.reader.name
  privileges        = ["SELECT"]
  on_schema_object {
    future {
      object_type_plural = each.value
      in_schema          = snowflake_schema.marts.fully_qualified_name
    }
  }
}

# Service users: key-pair auth only; snowflake_service_user has no password argument.
resource "snowflake_service_user" "dbt" {
  name              = "OFD_DBT"
  rsa_public_key    = var.dbt_public_key
  default_role      = snowflake_account_role.transformer.name
  default_warehouse = snowflake_warehouse.ofd.name
  default_namespace = "${snowflake_database.ofd.name}.${snowflake_schema.staging.name}"
}

resource "snowflake_service_user" "dashboard" {
  name              = "OFD_DASHBOARD"
  rsa_public_key    = var.dashboard_public_key
  default_role      = snowflake_account_role.reader.name
  default_warehouse = snowflake_warehouse.ofd.name
  default_namespace = "${snowflake_database.ofd.name}.${snowflake_schema.marts.name}"
}

resource "snowflake_grant_account_role" "dbt" {
  role_name = snowflake_account_role.transformer.name
  user_name = snowflake_service_user.dbt.name
}

resource "snowflake_grant_account_role" "dashboard" {
  role_name = snowflake_account_role.reader.name
  user_name = snowflake_service_user.dashboard.name
}
