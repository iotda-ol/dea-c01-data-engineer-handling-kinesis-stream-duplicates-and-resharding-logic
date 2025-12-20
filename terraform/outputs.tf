output "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream"
  value       = aws_kinesis_stream.inventory_stream.name
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis Data Stream"
  value       = aws_kinesis_stream.inventory_stream.arn
}

output "dynamodb_table_name" {
  description = "Name of the DynamoDB idempotency table"
  value       = aws_dynamodb_table.idempotency_table.name
}

output "dynamodb_table_arn" {
  description = "ARN of the DynamoDB idempotency table"
  value       = aws_dynamodb_table.idempotency_table.arn
}

output "producer_role_arn" {
  description = "ARN of the IAM role for the producer"
  value       = aws_iam_role.producer_role.arn
}

output "consumer_role_arn" {
  description = "ARN of the IAM role for the consumer"
  value       = aws_iam_role.consumer_role.arn
}

output "aws_region" {
  description = "AWS region where resources are created"
  value       = var.aws_region
}
