variable "snowflake_organization_name" {
  type        = string
  default     = null
  description = "Snowflake organization name (the part before the dash in myorg-myaccount). Null reads SNOWFLAKE_ORGANIZATION_NAME."
}

variable "snowflake_account_name" {
  type        = string
  default     = null
  description = "Snowflake account name (the part after the dash in myorg-myaccount). Null reads SNOWFLAKE_ACCOUNT_NAME."
}

variable "snowflake_admin_user" {
  type        = string
  default     = null
  description = "Admin user Terraform connects as. Null reads SNOWFLAKE_USER."
}

variable "snowflake_admin_password" {
  type        = string
  default     = null
  sensitive   = true
  description = "Password of the admin user. Null reads SNOWFLAKE_PASSWORD. Only the admin uses a password; the OFD_* users are key-pair only."
}

variable "snowflake_admin_role" {
  type        = string
  default     = "ACCOUNTADMIN"
  description = "Role Terraform uses. Creating the resource monitor needs ACCOUNTADMIN."
}

variable "monthly_credit_quota" {
  type        = number
  default     = 10
  description = "Monthly credit quota of the resource monitor. The warehouse is suspended at 100 percent."
}

variable "dbt_public_key" {
  type        = string
  description = "RSA public key of OFD_DBT: one line, without the BEGIN/END PUBLIC KEY header and footer."
}

variable "dashboard_public_key" {
  type        = string
  description = "RSA public key of OFD_DASHBOARD: one line, without the BEGIN/END PUBLIC KEY header and footer."
}

variable "databricks_host" {
  type        = string
  default     = ""
  description = "Databricks workspace URL (terraform -chdir=infra/databricks output databricks_host). Snowflake reads Silver from its Unity Catalog Iceberg REST endpoint."
}

variable "uc_catalog_name" {
  type        = string
  default     = "outfordelivery"
  description = "Unity Catalog catalog that holds the silver schema."
}

variable "uc_oauth_client_id" {
  type        = string
  default     = ""
  description = "Application ID of the Databricks service principal (infra/databricks output snowflake_oauth_client_id)."
}

variable "uc_oauth_client_secret" {
  type        = string
  default     = ""
  sensitive   = true
  description = "OAuth secret of that service principal (infra/databricks output snowflake_oauth_client_secret)."
}

variable "source_mode" {
  type        = string
  default     = "native"
  description = "Native snapshot copy through the device, or optional compatible-storage Iceberg."
  validation {
    condition     = contains(["native", "iceberg"], var.source_mode)
    error_message = "source_mode must be native or iceberg."
  }
}

variable "loader_public_key" {
  type        = string
  default     = ""
  description = "RSA public key for the dedicated native-mode OFD_LOADER identity. Private keys stay outside the repo."
}
