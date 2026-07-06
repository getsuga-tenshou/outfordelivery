resource "aws_iam_role" "glue" {
  name = "${local.name}-glue-${local.suffix}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "glue.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_inline" {
  name = "ofd-glue-access"
  role = aws_iam_role.glue.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "Lake"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [aws_s3_bucket.lake.arn, "${aws_s3_bucket.lake.arn}/*", aws_s3_bucket.artifacts.arn, "${aws_s3_bucket.artifacts.arn}/*"]
      },
      {
        Sid      = "Serving"
        Effect   = "Allow"
        Action   = ["dynamodb:PutItem", "dynamodb:UpdateItem", "dynamodb:DescribeTable"]
        Resource = aws_dynamodb_table.parcel_state.arn
      },
      {
        Sid      = "Logs"
        Effect   = "Allow"
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}


resource "aws_security_group" "glue" {
  name   = "${local.name}-glue-${local.suffix}"
  vpc_id = data.aws_vpc.default.id
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_security_group_rule" "glue_self" {
  type              = "ingress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.glue.id
}


resource "aws_glue_connection" "kafka" {
  name            = "${local.name}-kafka-${local.suffix}"
  connection_type = "KAFKA"
  connection_properties = {
    KAFKA_BOOTSTRAP_SERVERS = "${aws_instance.kafka.private_ip}:29092"
  }
  physical_connection_requirements {
    availability_zone      = aws_instance.kafka.availability_zone
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = data.aws_subnets.default.ids[0]
  }
}


resource "aws_s3_object" "glue_script" {
  bucket = aws_s3_bucket.artifacts.id
  key    = "scripts/glue_streaming_job.py"
  source = "${path.module}/../streaming/glue_streaming_job.py"
  etag   = filemd5("${path.module}/../streaming/glue_streaming_job.py")
}

resource "aws_glue_job" "streaming" {
  name              = "${local.name}-streaming-${local.suffix}"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  connections       = [aws_glue_connection.kafka.name]

  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--datalake-formats"                 = "delta"
    "--OFD_LAKE"                         = "s3://${aws_s3_bucket.lake.bucket}"
    "--OFD_KAFKA"                        = "${aws_instance.kafka.private_ip}:29092"
    "--OFD_TOPIC"                        = "parcel.events"
    "--OFD_DDB_TABLE"                    = aws_dynamodb_table.parcel_state.name
    "--OFD_REGION"                       = var.region
    "--enable-metrics"                   = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}
