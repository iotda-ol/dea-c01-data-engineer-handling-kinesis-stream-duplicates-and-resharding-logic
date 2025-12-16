# Kinesis Producer

This directory contains the Python producer script that generates inventory records and sends them to Amazon Kinesis Data Streams.

## Features

- **UUID Generation**: Each inventory record has a unique identifier
- **Batch Processing**: Supports sending multiple records efficiently
- **Failure Simulation**: Toggle to simulate network timeouts and retries
- **Duplicate Generation**: Demonstrates "At-Least-Once" delivery challenges
- **Configurable Parameters**: Customizable stream name, region, and failure rate

## Installation

### Prerequisites
- Python 3.7 or higher
- AWS credentials configured (via AWS CLI, environment variables, or IAM role)

### Install Dependencies

```bash
cd producer
pip install -r requirements.txt
```

## Usage

### Basic Usage

Send 20 inventory records to the default stream:
```bash
python producer.py
```

### Custom Stream and Region

```bash
python producer.py --stream-name my-inventory-stream --region us-west-2
```

### Simulate Network Failures

Enable failure simulation with 20% failure rate:
```bash
python producer.py --simulate-failures --failure-rate 0.2
```

This will:
- Randomly simulate network timeouts
- Automatically retry failed requests
- Create potential duplicate records
- Log all retry attempts

### Continuous Mode

Run continuously, sending records at regular intervals:
```bash
python producer.py --continuous --interval 2.0 --simulate-failures
```

Press `Ctrl+C` to stop.

### All Options

```bash
python producer.py \
  --stream-name inventory-stream \
  --region us-east-1 \
  --num-records 50 \
  --simulate-failures \
  --failure-rate 0.3 \
  --interval 0.5 \
  --continuous
```

## Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--stream-name` | Name of the Kinesis stream | `inventory-stream` |
| `--region` | AWS region | `us-east-1` |
| `--num-records` | Number of records to generate | `20` |
| `--simulate-failures` | Enable failure simulation | `False` |
| `--failure-rate` | Probability of failure (0.0-1.0) | `0.2` |
| `--interval` | Seconds between records | `1.0` |
| `--continuous` | Run continuously | `False` |

## Record Format

Each inventory record contains:

```json
{
  "record_id": "550e8400-e29b-41d4-a716-446655440000",
  "product_id": "WIDGET-001",
  "quantity": 42,
  "warehouse": "WH-EAST",
  "timestamp": "2024-01-15T10:30:45.123456"
}
```

- **record_id**: UUID v4 (unique identifier for idempotency)
- **product_id**: Product identifier
- **quantity**: Stock quantity
- **warehouse**: Warehouse location
- **timestamp**: ISO 8601 timestamp (UTC)

## Failure Simulation

When `--simulate-failures` is enabled:

1. **Random Failures**: Records randomly fail based on `--failure-rate`
2. **Simulated Timeout**: Brief delay to simulate network latency
3. **Automatic Retry**: Producer retries the same record
4. **Duplicate Creation**: Same `record_id` sent multiple times
5. **Logging**: All retries are logged for visibility

### Example Output with Failures

```
2024-01-15 10:30:45 - __main__ - WARNING - Simulating network timeout for record: 550e8400-e29b-41d4-a716-446655440000
2024-01-15 10:30:45 - __main__ - INFO - Retrying record after timeout: 550e8400-e29b-41d4-a716-446655440000
2024-01-15 10:30:45 - __main__ - INFO - Sent record 550e8400-e29b-41d4-a716-446655440000 to shard shardId-000000000000
```

## DEA-C01 Certification Alignment

This producer demonstrates:

- **Domain 1 - Data Ingestion**: 
  - Kinesis Data Streams as a data source
  - Handling high-throughput data ingestion
  
- **Domain 2 - Data Transformation**:
  - Structured JSON data format
  - Partition key strategy for even distribution

- **Domain 3 - Data Durability**:
  - At-least-once delivery semantics
  - Producer retry behavior
  - Need for consumer-side idempotency

## Troubleshooting

### Authentication Errors

Ensure AWS credentials are configured:
```bash
aws configure
# or
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-1
```

### Stream Not Found

Verify the stream exists:
```bash
aws kinesis describe-stream --stream-name inventory-stream
```

### Throttling Errors

If using provisioned mode, increase shard count or use On-Demand mode.

## Testing

Test the producer without AWS:
```python
from producer import InventoryRecord, generate_sample_inventory

# Generate test records
records = generate_sample_inventory(5)
for record in records:
    print(record.to_json())
```

## Next Steps

After running the producer:
1. Verify records in Kinesis using AWS Console or CLI
2. Run the consumer to process records with idempotency
3. Observe duplicate handling in the consumer logs
