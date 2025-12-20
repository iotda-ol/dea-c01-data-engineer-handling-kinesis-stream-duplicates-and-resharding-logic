# Terraform Infrastructure

This directory contains Terraform configurations to provision the AWS infrastructure required for the DEA-C01 Kinesis Resiliency Lab.

## Resources Created

1. **Amazon Kinesis Data Stream** (On-Demand mode)
   - Automatic scaling based on throughput
   - Configurable retention period
   - Enhanced monitoring with shard-level metrics

2. **Amazon DynamoDB Table** (Pay-per-request billing)
   - Idempotency tracking table
   - TTL enabled for automatic cleanup
   - Point-in-time recovery enabled

3. **IAM Roles and Policies**
   - Producer role with Kinesis write permissions
   - Consumer role with Kinesis read and DynamoDB access

## Prerequisites

- Terraform >= 1.0
- AWS CLI configured with appropriate credentials
- AWS account with permissions to create the above resources

## Usage

### 1. Initialize Terraform

```bash
cd terraform
terraform init
```

### 2. Review and Customize Variables

Copy the example variables file:
```bash
cp terraform.tfvars.example terraform.tfvars
```

Edit `terraform.tfvars` to customize your deployment:
- `aws_region`: AWS region (default: us-east-1)
- `environment`: Environment name (default: dev)
- `kinesis_stream_name`: Name of the Kinesis stream
- `dynamodb_table_name`: Name of the DynamoDB table
- `retention_period_hours`: Data retention period (24-8760 hours)
- `enable_ttl`: Enable TTL for DynamoDB (default: true)
- `enable_pitr`: Enable Point-in-Time Recovery (default: true)

### 3. Plan the Deployment

```bash
terraform plan
```

### 4. Apply the Configuration

```bash
terraform apply
```

Review the planned changes and type `yes` to confirm.

### 5. View Outputs

After deployment, view the resource details:
```bash
terraform output
```

This will display:
- Kinesis stream name and ARN
- DynamoDB table name and ARN
- IAM role ARNs
- AWS region

## Clean Up

To destroy all resources:
```bash
terraform destroy
```

## Architecture Notes

### Kinesis On-Demand Mode
- Automatically scales to handle varying throughput
- No need to provision or manage shards manually
- Pay only for the data written and read

### DynamoDB Pay-Per-Request
- Scales automatically with workload
- No capacity planning required
- Ideal for unpredictable workloads

### Idempotency Table Schema
- **Primary Key**: `record_id` (String) - UUID of the inventory record
- **TTL Attribute**: `ttl` (Number) - Unix timestamp for automatic cleanup
- Additional attributes stored: `processed_at`, `shard_id`, `sequence_number`

## Cost Considerations

- **Kinesis On-Demand**: $0.04 per GB written, $0.08 per GB read
- **DynamoDB On-Demand**: $1.25 per million write requests, $0.25 per million read requests
- Adjust retention periods and TTL settings to optimize costs

## DEA-C01 Certification Alignment

This infrastructure demonstrates:
- **Domain 1**: Data ingestion with Kinesis Data Streams
- **Domain 2**: Data transformation and processing patterns
- **Domain 3**: Data durability and idempotency with DynamoDB
- **Domain 4**: Monitoring with CloudWatch metrics
