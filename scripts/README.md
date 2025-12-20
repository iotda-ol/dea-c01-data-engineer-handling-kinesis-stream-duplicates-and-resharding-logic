# Kinesis Resharding Scripts

This directory contains utility scripts for managing Kinesis stream resharding operations.

## Scripts

### reshard.py

Utility for splitting and merging shards in a Kinesis stream.

**Important Note**: Resharding operations (split/merge) are only available for **provisioned mode** streams. The On-Demand mode streams created by the Terraform configuration automatically scale and do not support manual resharding.

## Installation

```bash
cd scripts
pip install -r requirements.txt
```

## Prerequisites

- AWS credentials configured
- Kinesis stream must be in **provisioned mode** for split/merge operations
- IAM permissions for kinesis:SplitShard, kinesis:MergeShards, kinesis:UpdateShardCount

## Converting Stream to Provisioned Mode

If you want to test resharding with a stream created in On-Demand mode, you need to convert it:

```bash
aws kinesis update-stream-mode \
  --stream-arn arn:aws:kinesis:REGION:ACCOUNT:stream/STREAM_NAME \
  --stream-mode-details StreamMode=PROVISIONED
```

Then set the shard count:
```bash
python reshard.py --stream-name inventory-stream update-count --target-count 2
```

## Usage

### View Shard Information

Display detailed information about all shards:
```bash
python reshard.py --stream-name inventory-stream info
```

Output includes:
- Shard IDs
- Status (OPEN/CLOSED)
- Hash key ranges
- Sequence numbers
- Parent/child relationships

### Split a Shard

Split a single shard into two child shards:

```bash
# Split at midpoint (automatic)
python reshard.py --stream-name inventory-stream split --shard-id shardId-000000000000

# Split at specific hash key
python reshard.py --stream-name inventory-stream split \
  --shard-id shardId-000000000000 \
  --hash-key 170141183460469231731687303715884105728
```

**What happens**:
1. Parent shard closes (gets `EndingSequenceNumber`)
2. Two child shards are created
3. Each child handles half of the parent's hash key range
4. Stream status changes to UPDATING, then back to ACTIVE
5. Consumer automatically discovers and processes new shards

### Merge Adjacent Shards

Merge two adjacent shards into one:

```bash
python reshard.py --stream-name inventory-stream merge \
  --shard-id shardId-000000000001 \
  --adjacent-shard-id shardId-000000000002
```

**Requirements**:
- Both shards must be OPEN
- Shards must be adjacent (consecutive hash key ranges)

**What happens**:
1. Both parent shards close
2. New merged shard is created
3. Merged shard handles combined hash key range
4. Consumer adapts to the new topology

### Update Shard Count (Uniform Scaling)

Update the total number of shards:

```bash
# Scale up to 4 shards
python reshard.py --stream-name inventory-stream update-count --target-count 4

# Scale down to 1 shard
python reshard.py --stream-name inventory-stream update-count --target-count 1
```

**Limitations**:
- Target count must be between 1 and 10,000
- Each update can change shard count by max 50% of current count
- Only available for provisioned mode

## Examples

### Example 1: Scale Up for Higher Throughput

```bash
# Check current state
python reshard.py --stream-name inventory-stream info

# Scale from 1 to 2 shards
python reshard.py --stream-name inventory-stream update-count --target-count 2

# Verify new topology
python reshard.py --stream-name inventory-stream info
```

### Example 2: Manual Split for Testing

```bash
# Split a specific shard
python reshard.py --stream-name inventory-stream split \
  --shard-id shardId-000000000000

# Run consumer to observe behavior during resharding
cd ../consumer
python consumer.py

# Check results
cd ../scripts
python reshard.py --stream-name inventory-stream info
```

### Example 3: Merge to Reduce Costs

```bash
# List shards to identify adjacent pairs
python reshard.py --stream-name inventory-stream info

# Merge adjacent shards
python reshard.py --stream-name inventory-stream merge \
  --shard-id shardId-000000000001 \
  --adjacent-shard-id shardId-000000000002
```

## Understanding Shard States

### OPEN Shard
- Actively receiving data
- Can be split or merged
- Has no `EndingSequenceNumber`

### CLOSED Shard
- No longer receiving new data
- Result of resharding operation
- Has `EndingSequenceNumber`
- Consumer continues reading until exhausted

## Resharding Best Practices

1. **Monitor During Resharding**:
   - Stream status changes to UPDATING
   - Takes 1-5 minutes to complete
   - Producer and consumer continue operating

2. **Consumer Behavior**:
   - Automatically discovers new shards
   - Completes processing closed shards
   - No data loss during resharding

3. **Shard Limitations**:
   - Each shard: 1 MB/sec or 1,000 records/sec writes
   - Each shard: 2 MB/sec reads (5 transactions/sec)

4. **Cost Optimization**:
   - Right-size shard count for throughput
   - Consider On-Demand mode for variable workloads
   - Merge underutilized shards

## Testing Resharding with Consumer

### Test Scenario: Verify Idempotency During Resharding

```bash
# Terminal 1: Start producer with duplicates
cd producer
python producer.py --continuous --simulate-failures --interval 1

# Terminal 2: Start consumer
cd consumer
python consumer.py

# Terminal 3: Perform resharding
cd scripts
python reshard.py --stream-name inventory-stream split --shard-id shardId-000000000000

# Observe consumer logs - it should:
# - Detect closed parent shard
# - Discover new child shards
# - Continue processing without duplicates
```

## Output Format

### Info Command Output

```
================================================================================
SHARD INFORMATION FOR STREAM: inventory-stream
================================================================================

Shard ID: shardId-000000000000
  Status: CLOSED
  Hash Key Range: 0 - 340282366920938463463374607431768211455
  Starting Sequence: 49590338271490256608559692538361571095921575989136588810
  Ending Sequence: 49590338271490256608559692538372076021741190618311294986
  
Shard ID: shardId-000000000001
  Status: OPEN
  Hash Key Range: 0 - 170141183460469231731687303715884105727
  Starting Sequence: 49590338271490256608559692538374499488101502892836626466
  Parent Shard: shardId-000000000000

Shard ID: shardId-000000000002
  Status: OPEN
  Hash Key Range: 170141183460469231731687303715884105728 - 340282366920938463463374607431768211455
  Starting Sequence: 49590338271490256608559692538375708414281117522011332642
  Parent Shard: shardId-000000000000
================================================================================
```

## DEA-C01 Certification Alignment

This script demonstrates:

- **Domain 1 - Data Ingestion**:
  - Stream capacity management
  - Scaling strategies
  
- **Domain 2 - Data Transformation**:
  - Handling topology changes
  - Shard-level processing
  
- **Domain 3 - Data Durability**:
  - Zero data loss during resharding
  - Continuous processing during scaling

## Troubleshooting

### Error: "Stream is in ON_DEMAND mode"

**Solution**: Convert to provisioned mode:
```bash
aws kinesis update-stream-mode \
  --stream-arn <stream-arn> \
  --stream-mode-details StreamMode=PROVISIONED
```

### Error: "Shard is already closed"

**Solution**: You cannot split/merge closed shards. Use `info` command to identify OPEN shards.

### Error: "InvalidArgumentException: Target shard count too high"

**Solution**: Scale incrementally. Each update can change shard count by max 50%.

### Timeout Waiting for Stream Active

**Cause**: Resharding takes time (1-5 minutes)

**Solution**: Script waits automatically. If it times out, check stream status:
```bash
aws kinesis describe-stream --stream-name inventory-stream
```

## Advanced Usage

### Programmatic Access

Use the `KinesisReshardManager` class in your own scripts:

```python
from reshard import KinesisReshardManager

manager = KinesisReshardManager('inventory-stream', 'us-east-1')

# Get shard info
shards = manager.list_shards()

# Split a shard
success = manager.split_shard('shardId-000000000000')

# Merge shards
success = manager.merge_shards('shardId-000000000001', 'shardId-000000000002')
```

## Additional Resources

- [Kinesis Resharding Documentation](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-resharding.html)
- [Shard Limits and Quotas](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
- [On-Demand vs Provisioned Mode](https://docs.aws.amazon.com/streams/latest/dev/how-do-i-size-a-stream.html)
