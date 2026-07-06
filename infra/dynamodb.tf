resource "aws_dynamodb_table" "parcel_state" {
  name         = "${local.name}-parcel-state-${local.suffix}"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "parcel_id"

  attribute {
    name = "parcel_id"
    type = "S"
  }

  stream_enabled   = true
  stream_view_type = "NEW_IMAGE"
}
