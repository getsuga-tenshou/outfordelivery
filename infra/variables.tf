variable "project" {
  type        = string
  default     = "outfordelivery"
  description = "Name prefix for all resources."
}

variable "region" {
  type        = string
  default     = "eu-central-1"
  description = "AWS region (Frankfurt, your Phase 9 choice)."
}

variable "aws_profile" {
  type        = string
  default     = null
  description = "AWS CLI named profile. Leave null to use the default credential chain (env vars or ~/.aws)."
}

variable "instance_type" {
  type        = string
  default     = "t3.small"
  description = "EC2 instance type for the Kafka/Redpanda node."
}

variable "ec2_key_name" {
  type        = string
  default     = null
  description = "Name of an existing EC2 key pair for SSH to the Kafka node. Null attaches no key."
}

variable "allowed_cidr" {
  type        = string
  description = "Your public IP in CIDR form, e.g. 203.0.113.4/32 (see https://checkip.amazonaws.com), for SSH and Kafka access. Do not use 0.0.0.0/0."
}

variable "alert_email" {
  type        = string
  description = "Email subscribed to the SLA-breach SNS topic. You confirm the subscription from the mail AWS sends."
}

variable "enable_redshift" {
  type        = bool
  default     = false
  description = "Athena-only by default (your choice). Set true only if you later want Redshift Serverless as well."
}
