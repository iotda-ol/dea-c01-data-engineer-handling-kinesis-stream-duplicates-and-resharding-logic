# Kinesis Consumer with Idempotency

This directory contains the Python consumer script that processes inventory records from Amazon Kinesis Data Streams using a "Check-and-Set" pattern with DynamoDB for exactly-once processing semantics.

## Features

- **Check-and-Set Pattern**: Atomic conditional writes to DynamoDB for idempotency
- **Exactly-Once Processing**: Ensures each record is processed only once
- **Duplicate Detection**: Handles duplicates from producer retries
- **Resharding Support**: Gracefully handles shard splits and merges
- **TTL Management**: Automatic cleanup of old idempotency records
- **Comprehensive Logging**: Detailed logging of all operations

## Architecture

### Check-and-Set Pattern

```
┌─────────────┐
│   Kinesis   │
│   Record    │
└──────┬──────┘
       │
       ▼
┌─────────────────────────┐
│ 1. Extract record_id    │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────────────────┐
│ 2. Conditional PutItem to DynamoDB  │
│    (if record_id NOT EXISTS)        │
└──────┬──────────────────────────────┘
       │
       ├─── Success ───┐
       │               ▼
       │         ┌──────────────┐
       │         │  NEW RECORD  │
       │         │   PROCESS    │
       │         └──────────────┘
       │
       └─── ConditionalCheckFailed ───┐
                                      ▼
                               ┌──────────────┐
                               │  DUPLICATE   │
                               │     SKIP     │
                               └──────────────┘
```

### Why This Pattern Works

1. **Atomic Operation**: DynamoDB's conditional write is atomic
2. **Race Condition Safe**: Multiple consumers can't process the same record
3. **Shard Resharding**: Works across shard splits and merges
4. **Producer Retries**: Handles duplicate sends from producer failures

## Installation

### Prerequisites
- Python 3.7 or higher
- AWS credentials configured
- DynamoDB table created (via Terraform)
- Kinesis stream with data

### Install Dependencies

```bash
cd consumer
pip install -r requirements.txt
```

## Usage

### Basic Usage

Process records from the default stream:
```bash
python consumer.py
```

### Custom Configuration

```bash
python consumer.py \
  --stream-name my-inventory-stream \
  --table-name my-idempotency-table \
  --region us-west-2
```

### Testing with Limited Iterations

Process only a few batches for testing:
```bash
python consumer.py --max-iterations 10
```

### Process Specific Shard

For testing or debugging:
```bash
python consumer.py --shard-id shardId-000000000000
```

## Command-Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| `--stream-name` | Name of the Kinesis stream | `inventory-stream` |
| `--table-name` | Name of the DynamoDB table | `kinesis-idempotency-table` |
| `--region` | AWS region | `us-east-1` |
| `--app-name` | Application name | `inventory-consumer` |
| `--max-iterations` | Max iterations per shard | `None` (unlimited) |
| `--shard-id` | Process only specific shard | `None` (all shards) |

## How It Works

### 1. Record Reception

Consumer retrieves records from Kinesis shards:
```python
records = kinesis_client.get_records(ShardIterator=iterator)
```

### 2. Idempotency Check (Check-and-Set)

Before processing, attempt to insert record_id into DynamoDB:
```python
table.put_item(
    Item={
        'record_id': record_id,
        'shard_id': shard_id,
        'sequence_number': sequence_number,
        'processed_at': timestamp,
        'ttl': ttl_timestamp
    },
    ConditionExpression='attribute_not_exists(record_id)'
)
```

**Outcomes**:
- **Success**: Record is NEW → Process it
- **ConditionalCheckFailed**: Record is DUPLICATE → Skip it

### 3. Business Logic Processing

If not a duplicate, process the inventory update:
```python
processor.process_record(record_data)
```

### 4. Shard Monitoring

Consumer detects and handles:
- **Active Shards**: Normal processing
- **Closed Shards**: Stops processing (resharding occurred)
- **New Shards**: Automatically discovered in next iteration

## DynamoDB Table Schema

The idempotency table stores:

| Attribute | Type | Description |
|-----------|------|-------------|
| `record_id` | String (PK) | UUID from the record |
| `shard_id` | String | Source shard ID |
| `sequence_number` | String | Kinesis sequence number |
| `processed_at` | String | ISO 8601 timestamp |
| `ttl` | Number | Unix timestamp for TTL |

### TTL (Time To Live)

- Default: 7 days
- Automatically deletes old records
- Reduces storage costs
- Configurable per deployment

## Example Output

```
2024-01-15 10:35:00 - __main__ - INFO - Initialized Kinesis consumer for stream: inventory-stream
2024-01-15 10:35:00 - __main__ - INFO - Found 2 active shard(s)
2024-01-15 10:35:00 - __main__ - INFO - Starting to consume shard: shardId-000000000000
2024-01-15 10:35:01 - __main__ - INFO - Retrieved 5 record(s) from shard shardId-000000000000
2024-01-15 10:35:01 - __main__ - INFO - Received record 550e8400-e29b-41d4-a716-446655440000
2024-01-15 10:35:01 - __main__ - INFO - Record 550e8400-e29b-41d4-a716-446655440000 is NEW - processing
2024-01-15 10:35:01 - __main__ - INFO - Processing record 550e8400-e29b-41d4-a716-446655440000: Product=WIDGET-001, Quantity=42, Warehouse=WH-EAST
2024-01-15 10:35:01 - __main__ - INFO - Successfully processed record 550e8400-e29b-41d4-a716-446655440000
2024-01-15 10:35:02 - __main__ - INFO - Received record 550e8400-e29b-41d4-a716-446655440000
2024-01-15 10:35:02 - __main__ - WARNING - Record 550e8400-e29b-41d4-a716-446655440000 is DUPLICATE - skipping
2024-01-15 10:35:02 - __main__ - INFO - Skipping duplicate record 550e8400-e29b-41d4-a716-446655440000
```

## Handling Resharding Events

### Shard Split

When a shard splits:
1. Parent shard closes (gets `EndingSequenceNumber`)
2. Consumer detects closed shard
3. New child shards are discovered
4. Consumer starts reading from child shards
5. Idempotency ensures no duplicate processing

### Shard Merge

When shards merge:
1. Source shards close
2. New merged shard becomes active
3. Consumer reads from the new shard
4. Check-and-Set prevents duplicate processing

## DEA-C01 Certification Alignment

This consumer demonstrates:

- **Domain 2 - Data Transformation**:
  - Stream processing patterns
  - Stateful processing with DynamoDB
  
- **Domain 3 - Data Durability**:
  - Exactly-once processing semantics
  - Idempotency patterns
  - Handling duplicate records
  - Conditional writes for consistency

- **Domain 4 - Monitoring**:
  - Detailed logging for observability
  - Statistics tracking

## Best Practices Implemented

1. **Atomic Idempotency**: Using conditional writes
2. **Graceful Degradation**: Continues on errors
3. **TTL Management**: Automatic cleanup
4. **Shard Awareness**: Handles resharding
5. **Rate Limiting**: Prevents throttling
6. **Comprehensive Logging**: Full audit trail

## Troubleshooting

### No Records Processing

Check if producer is running and stream has data:
```bash
aws kinesis describe-stream --stream-name inventory-stream
```

### ConditionalCheckFailed Errors

This is **expected** for duplicates. If all records fail:
- Verify table name is correct
- Check IAM permissions for DynamoDB

### Throttling Errors

Increase sleep time between GetRecords calls or use DynamoDB On-Demand mode.

## Production Considerations

For production deployments:

1. **Use AWS KCL (Kinesis Client Library)**:
   - Automatic shard assignment
   - Built-in checkpointing
   - Distributed processing

2. **Implement Checkpointing**:
   - Track processing progress
   - Resume from last position on restart

3. **Add Monitoring**:
   - CloudWatch metrics
   - DynamoDB capacity metrics
   - Processing latency tracking

4. **Scale Horizontally**:
   - Multiple consumer instances
   - One worker per shard maximum

5. **Error Handling**:
   - Dead letter queue for failed records
   - Retry policies
   - Alerting

## Testing

Test the consumer components:
```python
from consumer import IdempotencyChecker

# Test idempotency checker
checker = IdempotencyChecker('test-table')
is_dup = checker.is_duplicate('test-id-123', 'shard-001', '12345')
print(f"Is duplicate: {is_dup}")  # False first time, True second time
```

## Next Steps

1. Run producer to generate test data with duplicates
2. Run consumer to process records
3. Observe duplicate detection in logs
4. Test with resharding script
5. Verify inventory state is correct (no double-counting)
