variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "kinesis_stream_name" {
  description = "Name of the Kinesis Data Stream"
  type        = string
  default     = "inventory-stream"
}

variable "retention_period_hours" {
  description = "Data retention period in hours (24-8760)"
  type        = number
  default     = 24
  validation {
    condition     = var.retention_period_hours >= 24 && var.retention_period_hours <= 8760
    error_message = "Retention period must be between 24 and 8760 hours."
  }
}

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB idempotency table"
  type        = string
  default     = "kinesis-idempotency-table"
}

variable "enable_ttl" {
  description = "Enable TTL for automatic cleanup of old records"
  type        = bool
  default     = true
}

variable "enable_pitr" {
  description = "Enable Point-in-Time Recovery for DynamoDB"
  type        = bool
  default     = true
}
