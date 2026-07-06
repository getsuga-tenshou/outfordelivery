output "lake_bucket" {
  value       = aws_s3_bucket.lake.bucket
  description = "S3 data lake bucket (bronze, silver, gold)."
}

output "artifacts_bucket" {
  value       = aws_s3_bucket.artifacts.bucket
  description = "Bucket holding the Glue job script."
}

output "athena_results_bucket" {
  value       = aws_s3_bucket.athena_results.bucket
  description = "Bucket for Athena query results."
}

output "kafka_public_ip" {
  value       = aws_instance.kafka.public_ip
  description = "Public IP of the Kafka/Redpanda node."
}

output "kafka_bootstrap_external" {
  value       = "${aws_instance.kafka.public_ip}:9092"
  description = "Bootstrap address to reach Kafka from your laptop (within allowed_cidr)."
}

output "glue_job_name" {
  value       = aws_glue_job.streaming.name
  description = "Glue streaming job name."
}

output "glue_database" {
  value       = aws_glue_catalog_database.ofd.name
  description = "Glue Data Catalog database."
}

output "dynamodb_table" {
  value       = aws_dynamodb_table.parcel_state.name
  description = "DynamoDB serving table."
}

output "athena_workgroup" {
  value       = aws_athena_workgroup.ofd.name
  description = "Athena workgroup."
}

output "sns_topic_arn" {
  value       = aws_sns_topic.sla_breach.arn
  description = "SNS topic for SLA-breach alerts."
}
