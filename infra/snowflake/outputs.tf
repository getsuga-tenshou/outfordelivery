output "account_identifier" {
  value = var.snowflake_organization_name != null && var.snowflake_account_name != null ? "${var.snowflake_organization_name}-${var.snowflake_account_name}" : null
}

output "source_mode" {
  value = var.source_mode
}

output "loader_role" {
  value = var.source_mode == "native" ? snowflake_account_role.loader[0].name : null
}

output "loader_user" {
  value = var.source_mode == "native" ? snowflake_service_user.loader[0].name : null
}

output "warehouse" {
  value = snowflake_warehouse.ofd.name
}

output "database" {
  value = snowflake_database.ofd.name
}

output "roles" {
  value = {
    transformer = snowflake_account_role.transformer.name
    reader      = snowflake_account_role.reader.name
  }
}

output "users" {
  value = {
    dbt       = snowflake_service_user.dbt.name
    dashboard = snowflake_service_user.dashboard.name
  }
}
