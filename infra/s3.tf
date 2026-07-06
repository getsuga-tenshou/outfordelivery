resource "aws_s3_bucket" "lake" {
  bucket        = "${local.name}-lake-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "artifacts" {
  bucket        = "${local.name}-artifacts-${local.suffix}"
  force_destroy = true
}

resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.name}-athena-results-${local.suffix}"
  force_destroy = true
}

locals {
  bucket_ids = {
    lake           = aws_s3_bucket.lake.id
    artifacts      = aws_s3_bucket.artifacts.id
    athena_results = aws_s3_bucket.athena_results.id
  }
}


resource "aws_s3_bucket_public_access_block" "all" {
  for_each                = local.bucket_ids
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}


resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = local.bucket_ids
  bucket   = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}


resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}
