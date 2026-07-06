resource "aws_glue_catalog_database" "ofd" {
  name        = replace("${local.name}_${local.suffix}", "-", "_")
  description = "outfordelivery lake and warehouse catalog"
}
