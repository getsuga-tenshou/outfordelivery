terraform {
  required_version = ">= 1.5"
  required_providers {
    databricks = { source = "databricks/databricks", version = "~> 1.139" }
    aws        = { source = "hashicorp/aws", version = "~> 5.60" }
    time       = { source = "hashicorp/time", version = "~> 0.12" }
  }
}
provider "aws" {
  region              = var.aws_region
  profile             = var.aws_profile
  allowed_account_ids = [var.aws_account_id]
}
provider "databricks" { host = var.databricks_host }
locals {
  role_name = "${var.project}-uc-access"
  uc_prefix = "uc"
  # Built as a string so the storage credential does not depend on the role, which needs the credential's external ID.
  role_arn = "arn:aws:iam::${var.aws_account_id}:role/${local.role_name}"
}

# The credential is created before the role, so Unity Catalog cannot validate it yet.
# The external location below validates access once the role and policy exist.
resource "databricks_storage_credential" "lake" {
  name            = "${var.project}-lake"
  skip_validation = true
  comment         = "Managed by Terraform (infra/databricks)"

  aws_iam_role {
    role_arn = local.role_arn
  }
}

data "databricks_aws_unity_catalog_assume_role_policy" "lake" {
  aws_account_id = var.aws_account_id
  role_name      = local.role_name
  external_id    = databricks_storage_credential.lake.aws_iam_role[0].external_id
}

resource "aws_iam_role" "uc" {
  name               = local.role_name
  assume_role_policy = data.databricks_aws_unity_catalog_assume_role_policy.lake.json
}

resource "aws_iam_role_policy" "uc_lake" {
  name = "ofd-uc-lake-access"
  role = aws_iam_role.uc.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "UcObjects"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = "arn:aws:s3:::${var.lake_bucket}/${local.uc_prefix}/*"
      },
      {
        Sid      = "UcBucket"
        Effect   = "Allow"
        Action   = ["s3:ListBucket", "s3:GetBucketLocation"]
        Resource = "arn:aws:s3:::${var.lake_bucket}"
        Condition = {
          StringLike = { "s3:prefix" = [local.uc_prefix, "${local.uc_prefix}/*"] }
        }
      },
      {
        Sid      = "SelfAssume"
        Effect   = "Allow"
        Action   = "sts:AssumeRole"
        Resource = local.role_arn
      }
    ]
  })
}

# IAM changes take a few seconds to propagate; without this the external location validation can fail.
resource "time_sleep" "iam_propagation" {
  create_duration = "20s"
  depends_on      = [aws_iam_role_policy.uc_lake]
}

resource "databricks_external_location" "lake" {
  name            = "${var.project}-lake"
  url             = "s3://${var.lake_bucket}/${local.uc_prefix}"
  credential_name = databricks_storage_credential.lake.id
  comment         = "Managed by Terraform (infra/databricks)"
  depends_on      = [time_sleep.iam_propagation]
}

output "url" { value = databricks_external_location.lake.url }
output "iam_role_arn" { value = aws_iam_role.uc.arn }
