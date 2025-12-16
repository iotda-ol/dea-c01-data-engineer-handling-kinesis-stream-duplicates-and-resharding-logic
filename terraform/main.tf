terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Kinesis Data Stream with On-Demand capacity mode
resource "aws_kinesis_stream" "inventory_stream" {
  name = var.kinesis_stream_name

  # On-Demand capacity mode for automatic scaling
  stream_mode_details {
    stream_mode = "ON_DEMAND"
  }

  retention_period = var.retention_period_hours

  shard_level_metrics = [
    "IncomingBytes",
    "IncomingRecords",
    "OutgoingBytes",
    "OutgoingRecords",
    "WriteProvisionedThroughputExceeded",
    "ReadProvisionedThroughputExceeded",
    "IteratorAgeMilliseconds"
  ]

  tags = {
    Name        = var.kinesis_stream_name
    Environment = var.environment
    Purpose     = "DEA-C01 Kinesis Resiliency Lab"
  }
}

# DynamoDB table for idempotency tracking
resource "aws_dynamodb_table" "idempotency_table" {
  name           = var.dynamodb_table_name
  billing_mode   = "PAY_PER_REQUEST" # On-demand billing
  hash_key       = "record_id"

  attribute {
    name = "record_id"
    type = "S"
  }

  # TTL for automatic cleanup of old records (optional)
  ttl {
    attribute_name = "ttl"
    enabled        = var.enable_ttl
  }

  # Point-in-time recovery for data protection
  point_in_time_recovery {
    enabled = var.enable_pitr
  }

  tags = {
    Name        = var.dynamodb_table_name
    Environment = var.environment
    Purpose     = "Idempotency tracking for Kinesis consumer"
  }
}

# IAM role for Kinesis producer
resource "aws_iam_role" "producer_role" {
  name = "${var.environment}-kinesis-producer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-kinesis-producer-role"
    Environment = var.environment
  }
}

# IAM policy for Kinesis producer
resource "aws_iam_role_policy" "producer_policy" {
  name = "${var.environment}-kinesis-producer-policy"
  role = aws_iam_role.producer_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:PutRecord",
          "kinesis:PutRecords",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListShards"
        ]
        Resource = aws_kinesis_stream.inventory_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      }
    ]
  })
}

# IAM role for Kinesis consumer
resource "aws_iam_role" "consumer_role" {
  name = "${var.environment}-kinesis-consumer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name        = "${var.environment}-kinesis-consumer-role"
    Environment = var.environment
  }
}

# IAM policy for Kinesis consumer
resource "aws_iam_role_policy" "consumer_policy" {
  name = "${var.environment}-kinesis-consumer-policy"
  role = aws_iam_role.consumer_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:ListStreams",
          "kinesis:SubscribeToShard"
        ]
        Resource = aws_kinesis_stream.inventory_stream.arn
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:CreateTable",
          "dynamodb:DescribeTable",
          "dynamodb:GetItem",
          "dynamodb:PutItem",
          "dynamodb:UpdateItem",
          "dynamodb:Scan",
          "dynamodb:Query",
          "dynamodb:DeleteItem"
        ]
        Resource = [
          aws_dynamodb_table.idempotency_table.arn,
          "${aws_dynamodb_table.idempotency_table.arn}/*"
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "cloudwatch:PutMetricData"
        ]
        Resource = "*"
      }
    ]
  })
}
