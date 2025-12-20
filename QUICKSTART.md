# Quick Start Guide

This guide will help you get the Kinesis Resiliency Lab up and running in 15 minutes.

## Prerequisites Checklist

- [ ] AWS Account with admin access
- [ ] AWS CLI installed and configured (`aws configure`)
- [ ] Terraform >= 1.0 installed
- [ ] Python >= 3.7 installed
- [ ] pip (Python package manager)

## Step-by-Step Setup

### 1. Deploy AWS Infrastructure (5 minutes)

```bash
# Navigate to terraform directory
cd terraform

# Initialize Terraform
terraform init

# Preview what will be created
terraform plan

# Create the resources (approve when prompted)
terraform apply

# Save the outputs - you'll need these
terraform output
```

**What gets created:**
- Kinesis Data Stream: `inventory-stream` (On-Demand mode)
- DynamoDB Table: `kinesis-idempotency-table` (Pay-Per-Request)
- IAM Roles: Producer and Consumer roles

**Cost**: ~$0.50/day for light testing workload

### 2. Set Up Python Environment (3 minutes)

```bash
# Go back to project root
cd ..

# Install producer dependencies
cd producer
pip install -r requirements.txt

# Install consumer dependencies  
cd ../consumer
pip install -r requirements.txt

# Install scripts dependencies
cd ../scripts
pip install -r requirements.txt

cd ..
```

### 3. Test the Producer (2 minutes)

```bash
# Go to producer directory
cd producer

# Test without duplicates first
python producer.py --num-records 5

# You should see output like:
# INFO - Sent record abc-123... to shard shardId-000000000000
```

### 4. Test the Consumer (2 minutes)

```bash
# In a new terminal, go to consumer directory
cd consumer

# Run consumer to process the test records
python consumer.py --max-iterations 5

# You should see:
# INFO - Record abc-123... is NEW - processing
# INFO - Successfully processed record abc-123...
```

### 5. Test Duplicate Handling (3 minutes)

Now test the main feature - duplicate detection!

**Terminal 1 - Producer with Failures:**
```bash
cd producer
python producer.py --continuous --simulate-failures --failure-rate 0.3 --interval 2
```

Watch for:
- `WARNING - Simulating network timeout for record: xxx`
- `INFO - Retrying record after timeout: xxx` (This creates duplicates!)

**Terminal 2 - Consumer:**
```bash
cd consumer
python consumer.py
```

Watch for:
- `INFO - Record xxx is NEW - processing` (First time seeing it)
- `WARNING - Record xxx is DUPLICATE - skipping` (Duplicate detected!)

### 6. Verify Idempotency (2 minutes)

Check the DynamoDB table to see processed records:

```bash
aws dynamodb scan \
  --table-name kinesis-idempotency-table \
  --max-items 5 \
  --output table
```

You should see records with:
- `record_id` (UUID)
- `processed_at` (timestamp)
- `shard_id`
- `ttl` (expiration time)

## Quick Tests

### Test 1: Basic Flow
```bash
# Terminal 1
cd producer && python producer.py --num-records 10

# Terminal 2
cd consumer && python consumer.py --max-iterations 3
```

**Expected**: All 10 records processed, 0 duplicates detected

### Test 2: Duplicates from Producer Retries
```bash
# Terminal 1
cd producer && python producer.py --num-records 20 --simulate-failures --failure-rate 0.5

# Terminal 2
cd consumer && python consumer.py --max-iterations 5
```

**Expected**: ~10 original records processed, ~10 duplicates detected and skipped

### Test 3: Continuous Processing
```bash
# Terminal 1
cd producer && python producer.py --continuous --simulate-failures --interval 1

# Terminal 2  
cd consumer && python consumer.py

# Run for 1 minute, then Ctrl+C both
```

**Expected**: Consumer logs show mix of NEW and DUPLICATE records

## Verification Commands

### Check Stream Status
```bash
aws kinesis describe-stream --stream-name inventory-stream
```

### Check Table Records Count
```bash
aws dynamodb describe-table --table-name kinesis-idempotency-table | grep ItemCount
```

### Check Recent Records
```bash
aws dynamodb scan --table-name kinesis-idempotency-table --max-items 10
```

### View Shard Information
```bash
cd scripts
python reshard.py --stream-name inventory-stream info
```

## Common Issues

### Issue: "Stream not found"
**Solution**: Wait 1-2 minutes after `terraform apply` for stream to become active

### Issue: "Access Denied" errors
**Solution**: Check AWS credentials: `aws sts get-caller-identity`

### Issue: No records in DynamoDB
**Solution**: Ensure producer ran successfully and consumer is running

### Issue: All records showing as duplicates
**Solution**: Stop consumer, delete DynamoDB items, restart consumer

## Understanding the Output

### Producer Output
```
INFO - Sent record 550e8400-... to shard shardId-000000000000 (Seq: 495903...)
```
- Record ID: UUID for idempotency
- Shard ID: Which shard received the record
- Sequence: Kinesis sequence number

### Consumer Output - NEW Record
```
INFO - Received record 550e8400-...
INFO - Record 550e8400-... is NEW - processing
INFO - Processing record 550e8400-...: Product=WIDGET-001, Quantity=42
INFO - Successfully processed record 550e8400-...
```

### Consumer Output - DUPLICATE Record
```
INFO - Received record 550e8400-...
WARNING - Record 550e8400-... is DUPLICATE - skipping
INFO - Skipping duplicate record 550e8400-...
```

## Key Metrics to Observe

Watch the final statistics from the consumer:

```
=== Consumer Statistics ===
Records processed: 100        # NEW records written to DynamoDB
Duplicates detected: 25       # DUPLICATEs skipped (Check-and-Set worked!)
Total processed: 100          # Business logic executed only once per unique record
Final inventory state: {...}  # Correct totals (no double-counting)
```

## Next Steps

1. **Experiment**: Try different failure rates (0.1, 0.5, 0.8)
2. **Monitor**: Check CloudWatch metrics in AWS Console
3. **Scale**: Increase producer interval to send more records
4. **Learn**: Read the detailed READMEs in each directory
5. **Extend**: Add custom business logic to the processor

## Clean Up

When done testing:

```bash
cd terraform
terraform destroy
```

Type `yes` to confirm deletion of all resources.

## Cost Estimate

For 1 hour of light testing:
- Kinesis Data Stream: $0.02 (On-Demand pricing)
- DynamoDB: $0.01 (Pay-Per-Request)
- **Total: ~$0.03/hour**

For 1 day of moderate testing:
- Kinesis: ~$0.40
- DynamoDB: ~$0.10
- **Total: ~$0.50/day**

## Success Criteria

You've successfully completed the quick start if:

✅ Producer sends records with UUIDs to Kinesis
✅ Producer simulates failures and retries (creates duplicates)
✅ Consumer reads from Kinesis
✅ Consumer detects duplicates using DynamoDB Check-and-Set
✅ Consumer skips duplicate processing
✅ Final inventory totals are correct (no double-counting)

## Need Help?

- Check the main [README.md](README.md) for architecture details
- Review component READMEs:
  - [Terraform Infrastructure](terraform/README.md)
  - [Producer Guide](producer/README.md)
  - [Consumer Guide](consumer/README.md)
  - [Resharding Scripts](scripts/README.md)

## Pro Tips

💡 **Tip 1**: Keep producer and consumer running in separate terminals for easy observation

💡 **Tip 2**: Use `--max-iterations` on consumer for quick tests without stopping manually

💡 **Tip 3**: Increase `--failure-rate` to 0.8 for more dramatic duplicate scenarios

💡 **Tip 4**: Check CloudWatch Logs for detailed debugging if needed

💡 **Tip 5**: Use `jq` to pretty-print DynamoDB scan results:
```bash
aws dynamodb scan --table-name kinesis-idempotency-table | jq
```

Happy Learning! 🚀
