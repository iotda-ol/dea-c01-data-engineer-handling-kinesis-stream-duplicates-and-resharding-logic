# DEA-C01: Kinesis Resiliency Lab

A comprehensive data engineering implementation demonstrating how to handle **"At-Least-Once"** delivery challenges in Amazon Kinesis Data Streams. This project showcases production-ready patterns for building resilient, idempotent data pipelines that can gracefully handle:

- **Producer Retries**: Duplicates caused by network timeouts and failures
- **Stream Resharding**: Shard splits and merges without data loss or duplicate processing
- **Exactly-Once Semantics**: Using the "Check-and-Set" pattern with DynamoDB

This lab aligns with AWS Certified Data Engineer - Associate (DEA-C01) certification domains, particularly around data ingestion, transformation, durability, and idempotency patterns.

## 🎯 Learning Objectives

By working through this lab, you will understand:

1. **At-Least-Once Delivery**: Why Kinesis provides at-least-once guarantees and how duplicates occur
2. **Idempotency Patterns**: Implementing Check-and-Set with DynamoDB for exactly-once processing
3. **Producer Resilience**: Handling network failures and automatic retries
4. **Consumer Resilience**: Processing records across resharding events without duplicates
5. **Operational Best Practices**: Monitoring, scaling, and cost optimization

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         KINESIS RESILIENCY LAB                          │
└─────────────────────────────────────────────────────────────────────────┘

┌──────────────┐         ┌─────────────────────────────────────────┐
│   PRODUCER   │         │         KINESIS STREAM                  │
│              │         │      (On-Demand Capacity)               │
│  • Generates ├────────▶│                                         │
│    UUIDs     │         │  ┌────────┐  ┌────────┐  ┌────────┐   │
│  • Simulates │         │  │Shard 1 │  │Shard 2 │  │Shard N │   │
│    Failures  │         │  └────────┘  └────────┘  └────────┘   │
│  • Retries   │         │                                         │
│  • Creates   │         │  • Automatic Scaling                    │
│    Duplicates│         │  • 24hr Retention                       │
└──────────────┘         └─────────────┬───────────────────────────┘
                                       │
                                       ▼
                         ┌─────────────────────────────┐
                         │        CONSUMER             │
                         │  (Idempotent Processing)    │
                         │                             │
                         │  1. Read from Kinesis       │
                         │  2. Extract record_id       │
                         │  3. Check DynamoDB ◄────────┼──────┐
                         │  4. Conditional Write       │      │
                         │     (Check-and-Set)         │      │
                         │  5. Process if NEW          │      │
                         │  6. Skip if DUPLICATE       │      │
                         └─────────────────────────────┘      │
                                                               │
                                                               │
                         ┌─────────────────────────────────────┴───┐
                         │      DYNAMODB TABLE                     │
                         │   (Idempotency Tracking)                │
                         │                                         │
                         │  PK: record_id (UUID)                   │
                         │  Attributes:                            │
                         │    • shard_id                           │
                         │    • sequence_number                    │
                         │    • processed_at                       │
                         │    • ttl (7 days)                       │
                         │                                         │
                         │  • Conditional Writes (Atomic)          │
                         │  • Pay-Per-Request Billing              │
                         │  • Auto-cleanup via TTL                 │
                         └─────────────────────────────────────────┘
```

## 📁 Project Structure

```
.
├── terraform/              # Infrastructure as Code
│   ├── main.tf            # Kinesis + DynamoDB resources
│   ├── variables.tf       # Configuration variables
│   ├── outputs.tf         # Resource outputs
│   └── README.md          # Infrastructure documentation
│
├── producer/              # Data Producer
│   ├── producer.py        # Producer with failure simulation
│   ├── requirements.txt   # Python dependencies
│   └── README.md          # Producer documentation
│
├── consumer/              # Data Consumer
│   ├── consumer.py        # Consumer with Check-and-Set
│   ├── requirements.txt   # Python dependencies
│   └── README.md          # Consumer documentation
│
├── scripts/               # Utility Scripts
│   ├── reshard.py         # Resharding management
│   ├── requirements.txt   # Python dependencies
│   └── README.md          # Scripts documentation
│
└── README.md              # This file
```

## 🚀 Quick Start

### Prerequisites

- **AWS Account** with appropriate permissions
- **AWS CLI** configured with credentials
- **Terraform** >= 1.0
- **Python** >= 3.7
- **boto3** Python SDK

### Step 1: Deploy Infrastructure

```bash
cd terraform

# Initialize Terraform
terraform init

# Review the plan
terraform plan

# Deploy resources
terraform apply
```

This creates:
- Amazon Kinesis Data Stream (On-Demand mode)
- DynamoDB table for idempotency
- IAM roles and policies

### Step 2: Install Dependencies

```bash
# Producer dependencies
cd producer
pip install -r requirements.txt

# Consumer dependencies
cd ../consumer
pip install -r requirements.txt

# Scripts dependencies
cd ../scripts
pip install -r requirements.txt
```

### Step 3: Run the Producer

Open a terminal and start the producer with failure simulation:

```bash
cd producer
python producer.py --continuous --simulate-failures --failure-rate 0.3
```

This will:
- Generate inventory records continuously
- Simulate network timeouts (30% failure rate)
- Automatically retry failed requests
- Create duplicate records with same UUIDs

### Step 4: Run the Consumer

Open another terminal and start the consumer:

```bash
cd consumer
python consumer.py
```

Observe the logs - you'll see:
- Records being processed
- Duplicates being detected and skipped
- Exactly-once processing guarantees maintained

### Step 5: Test Resharding (Optional)

For provisioned mode streams, test resharding:

```bash
cd scripts

# View current shard topology
python reshard.py --stream-name inventory-stream info

# Split a shard
python reshard.py --stream-name inventory-stream split --shard-id shardId-000000000000

# Observe consumer adapting to new shards
```

**Note**: Resharding requires converting the stream to provisioned mode.

## 🔍 Key Concepts Demonstrated

### 1. At-Least-Once Delivery

**Problem**: Kinesis provides at-least-once delivery guarantees. Records may be delivered multiple times due to:
- Producer retries after network failures
- Consumer retries after processing failures
- Resharding events (reading from overlapping shards)

**Solution**: Implement idempotent consumers that can process the same record multiple times safely.

### 2. Check-and-Set Pattern

**Implementation**:
```python
# Atomic conditional write to DynamoDB
table.put_item(
    Item={'record_id': uuid, 'processed_at': timestamp, ...},
    ConditionExpression='attribute_not_exists(record_id)'
)
```

**Behavior**:
- **First time**: Write succeeds → Process the record
- **Duplicate**: Write fails (ConditionalCheckFailed) → Skip the record

**Why it works**:
- DynamoDB conditional writes are atomic
- No race conditions between multiple consumers
- Works across resharding events

### 3. Producer Failure Simulation

The producer simulates real-world network issues:

```python
if random.random() < failure_rate:
    # Simulate timeout
    logger.warning("Network timeout - retrying")
    # Send the record again (same UUID) → Duplicate!
    kinesis_client.put_record(...)
```

This creates duplicates that the consumer must handle.

### 4. Resharding Resilience

When shards split or merge:
1. **Old shards close**: Get `EndingSequenceNumber`
2. **New shards open**: Start receiving records
3. **Consumer detects**: Discovers closed/new shards
4. **Processing continues**: Idempotency prevents duplicate processing

## 📊 Monitoring and Validation

### Check DynamoDB for Idempotency Records

```bash
aws dynamodb scan --table-name kinesis-idempotency-table --max-items 10
```

### Check Kinesis Metrics

```bash
aws kinesis describe-stream --stream-name inventory-stream
```

### Verify Exactly-Once Processing

Compare:
- Producer logs: Total records sent (including duplicates)
- Consumer logs: Unique records processed
- DynamoDB items: Should match unique records

## 🎓 DEA-C01 Certification Alignment

This lab demonstrates competencies across multiple DEA-C01 exam domains:

### Domain 1: Data Ingestion and Transformation (34%)
- ✅ Kinesis Data Streams for real-time ingestion
- ✅ Stream capacity planning (On-Demand vs Provisioned)
- ✅ Partition key strategies
- ✅ Producer retry behavior

### Domain 2: Data Store Management (26%)
- ✅ DynamoDB for idempotency tracking
- ✅ Conditional writes for consistency
- ✅ TTL for automatic data lifecycle management
- ✅ Pay-per-request billing optimization

### Domain 3: Data Operations and Support (22%)
- ✅ Monitoring stream health and metrics
- ✅ Handling resharding events
- ✅ Consumer scaling patterns
- ✅ Error handling and retry logic

### Domain 4: Data Security and Governance (18%)
- ✅ IAM roles and policies
- ✅ Least privilege access
- ✅ Data durability guarantees

## 🔧 Troubleshooting

### Producer Issues

**Error: Stream not found**
```bash
aws kinesis describe-stream --stream-name inventory-stream
```

**Solution**: Ensure Terraform deployment completed successfully.

### Consumer Issues

**Error: ConditionalCheckFailed for all records**

**Solution**: Check DynamoDB table name matches configuration.

**No records being processed**

**Solution**: 
1. Verify producer is running
2. Check stream has data: `aws kinesis describe-stream --stream-name inventory-stream`
3. Verify IAM permissions

### Resharding Issues

**Error: "Stream is in ON_DEMAND mode"**

**Solution**: Manual resharding requires provisioned mode. Convert stream:
```bash
aws kinesis update-stream-mode --stream-arn <arn> --stream-mode-details StreamMode=PROVISIONED
```

## 💡 Best Practices Implemented

1. **Idempotency**: Check-and-Set pattern with DynamoDB
2. **Resilience**: Graceful handling of failures and retries
3. **Scalability**: On-Demand capacity for automatic scaling
4. **Cost Optimization**: TTL for automatic data cleanup
5. **Observability**: Comprehensive logging throughout
6. **Security**: IAM roles with least privilege
7. **Documentation**: Extensive inline and external docs

## 🔄 Cleanup

To avoid ongoing AWS charges:

```bash
cd terraform
terraform destroy
```

This removes:
- Kinesis Data Stream
- DynamoDB table
- IAM roles and policies

## 📚 Additional Resources

### AWS Documentation
- [Kinesis Data Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/)
- [DynamoDB Conditional Writes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate)
- [DEA-C01 Exam Guide](https://aws.amazon.com/certification/certified-data-engineer-associate/)

### Related Patterns
- Event Sourcing
- CQRS (Command Query Responsibility Segregation)
- Lambda Architecture
- Kappa Architecture

## 🤝 Contributing

This is a learning lab. Feel free to:
- Extend with additional features
- Add monitoring dashboards
- Implement additional consumer patterns
- Add unit and integration tests

## 📝 License

This project is provided as-is for educational purposes.

## ✨ Acknowledgments

This lab demonstrates production-ready patterns used by data engineering teams at scale. The Check-and-Set pattern is particularly important for building resilient, exactly-once processing systems on AWS.
