resource "aws_sns_topic" "sla_breach" {
  name = "${local.name}-sla-breach-${local.suffix}"
}

resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.sla_breach.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

resource "aws_iam_role" "lambda" {
  name = "${local.name}-lambda-${local.suffix}"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_inline" {
  name = "ofd-lambda-access"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Sid = "Publish", Effect = "Allow", Action = "sns:Publish", Resource = aws_sns_topic.sla_breach.arn },
      {
        Sid      = "Stream"
        Effect   = "Allow"
        Action   = ["dynamodb:GetRecords", "dynamodb:GetShardIterator", "dynamodb:DescribeStream", "dynamodb:ListStreams"]
        Resource = "${aws_dynamodb_table.parcel_state.arn}/stream/*"
      }
    ]
  })
}

data "archive_file" "lambda" {
  type        = "zip"
  source_file = "${path.module}/lambda/breach_notifier.py"
  output_path = "${path.module}/lambda/breach_notifier.zip"
}

resource "aws_lambda_function" "breach" {
  function_name    = "${local.name}-breach-${local.suffix}"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.12"
  handler          = "breach_notifier.handler"
  filename         = data.archive_file.lambda.output_path
  source_code_hash = data.archive_file.lambda.output_base64sha256
  timeout          = 30

  environment {
    variables = { SNS_TOPIC_ARN = aws_sns_topic.sla_breach.arn }
  }
}

resource "aws_lambda_event_source_mapping" "ddb_stream" {
  event_source_arn  = aws_dynamodb_table.parcel_state.stream_arn
  function_name     = aws_lambda_function.breach.arn
  starting_position = "LATEST"
}

resource "aws_cloudwatch_event_rule" "maintenance" {
  name                = "${local.name}-maintenance-${local.suffix}"
  description         = "Daily maintenance tick (wire lake compaction or a dbt run here)."
  schedule_expression = "rate(1 day)"
}

resource "aws_cloudwatch_event_target" "maintenance" {
  rule = aws_cloudwatch_event_rule.maintenance.name
  arn  = aws_lambda_function.breach.arn
}

resource "aws_lambda_permission" "events" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.breach.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.maintenance.arn
}
