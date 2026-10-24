output "catalog_name" {
  value = databricks_catalog.ofd.name
}

output "external_location_url" {
  value = var.use_external_storage ? var.external_location_url : null
}

output "volume_path" {
  value = "/Volumes/${databricks_catalog.ofd.name}/${databricks_schema.ops.name}/${databricks_volume.raw.name}"
}

output "export_volume_path" {
  value = "/Volumes/${databricks_catalog.ofd.name}/${databricks_schema.ops.name}/${databricks_volume.exports.name}"
}

output "databricks_host" {
  value = var.databricks_host
}

output "snowflake_oauth_client_id" {
  description = "Application ID of the service principal Snowflake authenticates as."
  value       = var.enable_snowflake_link ? databricks_service_principal.snowflake[0].application_id : null
}

output "snowflake_oauth_client_secret" {
  description = "terraform output -raw snowflake_oauth_client_secret; goes into infra/snowflake as uc_oauth_client_secret."
  value       = var.enable_snowflake_link ? databricks_service_principal_secret.snowflake[0].secret : null
  sensitive   = true
}
