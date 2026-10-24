locals {
  native_columns = {
    SILVER_PARCEL_EVENTS = [
      { name = "EVENT_ID", type = "VARCHAR", nullable = false },
      { name = "PARCEL_ID", type = "VARCHAR", nullable = false },
      { name = "EVENT_TYPE", type = "VARCHAR", nullable = false },
      { name = "STATUS", type = "VARCHAR", nullable = false },
      { name = "HUB_ID", type = "VARCHAR", nullable = false },
      { name = "LAT", type = "FLOAT", nullable = false },
      { name = "LON", type = "FLOAT", nullable = false },
      { name = "EVENT_TS", type = "TIMESTAMP_NTZ(6)", nullable = false },
      { name = "VERSION", type = "NUMBER(38,0)", nullable = false },
      { name = "WEATHER_BAD", type = "BOOLEAN", nullable = true },
      { name = "EVENT_DATE", type = "DATE", nullable = false },
    ]
    SILVER_PARCELS = [
      { name = "PARCEL_ID", type = "VARCHAR", nullable = false },
      { name = "ORIGIN_HUB", type = "VARCHAR", nullable = false },
      { name = "DESTINATION_POSTCODE", type = "VARCHAR", nullable = false },
      { name = "DESTINATION_REGION", type = "VARCHAR", nullable = false },
      { name = "CARRIER", type = "VARCHAR", nullable = false },
      { name = "SERVICE_LEVEL", type = "VARCHAR", nullable = false },
      { name = "SLA_HOURS", type = "NUMBER(38,0)", nullable = false },
      { name = "CREATED_AT", type = "TIMESTAMP_NTZ(6)", nullable = false },
      { name = "PROMISED_BY", type = "TIMESTAMP_NTZ(6)", nullable = false },
    ]
  }
  native_objects = merge(local.native_columns, {
    for name, columns in local.native_columns : "LOADING_${name}" => columns
    }, {
    LOAD_AUDIT = [
      { name = "EXPORT_ID", type = "VARCHAR", nullable = false },
      { name = "MANIFEST_HASH", type = "VARCHAR", nullable = false },
      { name = "LOADED_AT", type = "TIMESTAMP_NTZ(6)", nullable = false },
      { name = "TIME_ACCELERATION", type = "FLOAT", nullable = false },
      { name = "EVENTS_VERSION", type = "NUMBER(38,0)", nullable = false },
      { name = "PARCELS_VERSION", type = "NUMBER(38,0)", nullable = false },
      { name = "EVENTS_COUNT", type = "NUMBER(38,0)", nullable = false },
      { name = "PARCELS_COUNT", type = "NUMBER(38,0)", nullable = false },
      { name = "EVENT_MIN", type = "TIMESTAMP_NTZ(6)", nullable = true },
      { name = "EVENT_MAX", type = "TIMESTAMP_NTZ(6)", nullable = true },
      { name = "MANIFEST_JSON", type = "VARCHAR", nullable = false },
    ]
    LOAD_LOCK = [
      { name = "ID", type = "NUMBER(38,0)", nullable = false },
      { name = "OWNER", type = "VARCHAR", nullable = false },
    ]
  })
}

resource "snowflake_table" "native" {
  for_each = var.source_mode == "native" ? local.native_objects : {}
  database = snowflake_database.ofd.name
  schema   = snowflake_schema.lake.name
  name     = each.key
  dynamic "column" {
    for_each = each.value
    content {
      name     = column.value.name
      type     = column.value.type
      nullable = column.value.nullable
    }
  }
}

resource "snowflake_execute" "seed_load_lock" {
  provider = snowflake.bootstrap
  count    = var.source_mode == "native" ? 1 : 0
  execute  = "INSERT INTO ${snowflake_table.native["LOAD_LOCK"].fully_qualified_name} (ID, OWNER) SELECT 1, 'provisioned' WHERE NOT EXISTS (SELECT 1 FROM ${snowflake_table.native["LOAD_LOCK"].fully_qualified_name})"
  revert   = "SELECT 1"
}

resource "snowflake_file_format_parquet" "silver" {
  count                  = var.source_mode == "native" ? 1 : 0
  database               = snowflake_database.ofd.name
  schema                 = snowflake_schema.lake.name
  name                   = "SILVER_PARQUET"
  use_logical_type       = true
  binary_as_text         = false
  use_vectorized_scanner = false
}

resource "snowflake_stage_internal" "exports" {
  count    = var.source_mode == "native" ? 1 : 0
  database = snowflake_database.ofd.name
  schema   = snowflake_schema.lake.name
  name     = "SILVER_EXPORTS"
}

resource "snowflake_account_role" "loader" {
  count = var.source_mode == "native" ? 1 : 0
  name  = "OFD_LOADER"
}

resource "snowflake_service_user" "loader" {
  count             = var.source_mode == "native" ? 1 : 0
  name              = "OFD_LOADER"
  rsa_public_key    = var.loader_public_key
  default_role      = snowflake_account_role.loader[0].name
  default_warehouse = snowflake_warehouse.ofd.name
  default_namespace = "${snowflake_database.ofd.name}.${snowflake_schema.lake.name}"
  lifecycle {
    precondition {
      condition     = var.loader_public_key != ""
      error_message = "Native mode requires a dedicated loader_public_key; do not reuse an administrator or dashboard identity."
    }
  }
}

resource "snowflake_grant_account_role" "loader" {
  count     = var.source_mode == "native" ? 1 : 0
  role_name = snowflake_account_role.loader[0].name
  user_name = snowflake_service_user.loader[0].name
}

resource "snowflake_grant_privileges_to_account_role" "loader_usage" {
  for_each = var.source_mode == "native" ? {
    database  = { type = "DATABASE", name = snowflake_database.ofd.name }
    warehouse = { type = "WAREHOUSE", name = snowflake_warehouse.ofd.name }
  } : {}
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = each.value.type
    object_name = each.value.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_schema" {
  count             = var.source_mode == "native" ? 1 : 0
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = snowflake_schema.lake.fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_tables" {
  for_each          = var.source_mode == "native" ? local.native_objects : {}
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = each.key == "LOAD_LOCK" ? ["SELECT", "UPDATE"] : (each.key == "LOAD_AUDIT" ? ["SELECT", "INSERT"] : ["SELECT", "INSERT", "DELETE"])
  on_schema_object {
    object_type = "TABLE"
    object_name = snowflake_table.native[each.key].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_stage_read" {
  count             = var.source_mode == "native" ? 1 : 0
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = ["READ"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_internal.exports[0].fully_qualified_name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_stage_write" {
  count             = var.source_mode == "native" ? 1 : 0
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = ["WRITE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = snowflake_stage_internal.exports[0].fully_qualified_name
  }
  depends_on = [snowflake_grant_privileges_to_account_role.loader_stage_read]
}

resource "snowflake_grant_privileges_to_account_role" "loader_format" {
  count             = var.source_mode == "native" ? 1 : 0
  account_role_name = snowflake_account_role.loader[0].name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = snowflake_file_format_parquet.silver[0].fully_qualified_name
  }
}

# Explicit SELECT complements existing future grants, including creation in this same apply.
resource "snowflake_grant_privileges_to_account_role" "transformer_native" {
  for_each          = var.source_mode == "native" ? toset(["SILVER_PARCEL_EVENTS", "SILVER_PARCELS", "LOAD_AUDIT"]) : toset([])
  account_role_name = snowflake_account_role.transformer.name
  privileges        = ["SELECT"]
  on_schema_object {
    object_type = "TABLE"
    object_name = snowflake_table.native[each.value].fully_qualified_name
  }
}
