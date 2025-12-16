# DEA-C01 Exam Alignment

This document maps the Kinesis Resiliency Lab components to the AWS Certified Data Engineer - Associate (DEA-C01) exam domains.

## Exam Domain Coverage

### Domain 1: Data Ingestion and Transformation (34% of exam)

#### 1.1 Perform data ingestion

**Demonstrated Skills:**
- ✅ **Kinesis Data Streams**: Real-time data ingestion at scale
- ✅ **Partition Keys**: Strategic partitioning using product_id for even distribution
- ✅ **Producer Best Practices**: Retry logic, error handling, batch processing
- ✅ **Data Formats**: JSON serialization and schema design

**Lab Components:**
- `producer/producer.py`: Implements producer patterns
- `terraform/main.tf`: Provisions Kinesis stream with On-Demand capacity

**Exam Topics Covered:**
- Select appropriate ingestion services (Kinesis vs Kafka vs SQS)
- Configure partition keys for optimal throughput
- Implement producer retry logic
- Handle producer-side failures

#### 1.2 Transform data

**Demonstrated Skills:**
- ✅ **Stream Processing**: Processing records in real-time
- ✅ **Data Enrichment**: Adding metadata (timestamps, shard_id, sequence numbers)
- ✅ **Stateful Processing**: Maintaining inventory state across events

**Lab Components:**
- `consumer/consumer.py`: Processes and transforms inventory records
- Business logic in `InventoryProcessor` class

**Exam Topics Covered:**
- Process streaming data
- Maintain state during processing
- Handle data schema evolution

### Domain 2: Data Store Management (26% of exam)

#### 2.1 Choose a data store

**Demonstrated Skills:**
- ✅ **DynamoDB Selection**: Chose DynamoDB for idempotency tracking (low latency, atomic operations)
- ✅ **Kinesis Selection**: Chose Kinesis for stream ingestion (ordered, scalable, replay)
- ✅ **Capacity Modes**: On-Demand for both services (automatic scaling)

**Lab Components:**
- `terraform/main.tf`: Resource selection with justification in comments

**Exam Topics Covered:**
- Select appropriate data stores for use cases
- Compare NoSQL vs relational options
- Understand capacity modes (On-Demand vs Provisioned)

#### 2.2 Understand data cataloging systems

**Demonstrated Skills:**
- ✅ **Schema Design**: Well-defined inventory record schema
- ✅ **Metadata Management**: Tracking processing metadata in DynamoDB
- ✅ **Data Lineage**: Shard ID and sequence number tracking

**Lab Components:**
- Record schema in `producer/producer.py`
- Metadata tracking in `consumer/consumer.py`

#### 2.3 Manage the lifecycle of data

**Demonstrated Skills:**
- ✅ **TTL (Time To Live)**: Automatic cleanup of old idempotency records
- ✅ **Data Retention**: 24-hour retention on Kinesis stream
- ✅ **Cost Optimization**: Automatic deletion of expired data

**Lab Components:**
- TTL configuration in `terraform/main.tf`
- `ttl` attribute in idempotency records

**Exam Topics Covered:**
- Implement data retention policies
- Configure TTL for automatic cleanup
- Optimize storage costs

### Domain 3: Data Operations and Support (22% of exam)

#### 3.1 Automate data processing

**Demonstrated Skills:**
- ✅ **Infrastructure as Code**: Terraform for reproducible deployments
- ✅ **Automated Scaling**: On-Demand capacity for auto-scaling
- ✅ **Idempotent Operations**: Check-and-Set pattern for safe retries

**Lab Components:**
- `terraform/`: Complete IaC implementation
- Consumer idempotency logic

**Exam Topics Covered:**
- Use IaC tools (Terraform, CloudFormation)
- Implement idempotent data pipelines
- Design for automatic scaling

#### 3.2 Analyze data processing

**Demonstrated Skills:**
- ✅ **Metrics and Monitoring**: Shard-level metrics enabled
- ✅ **Logging**: Comprehensive logging throughout
- ✅ **Statistics Tracking**: Records processed, duplicates detected

**Lab Components:**
- CloudWatch metrics in `terraform/main.tf`
- Logging in all Python scripts
- Statistics methods in processor classes

**Exam Topics Covered:**
- Monitor streaming applications
- Track processing metrics
- Debug production issues

#### 3.3 Handle operational issues

**Demonstrated Skills:**
- ✅ **Error Handling**: Comprehensive exception handling
- ✅ **Retry Logic**: Exponential backoff for throttling
- ✅ **Graceful Degradation**: Continues processing on errors
- ✅ **Resharding Handling**: Adapts to topology changes

**Lab Components:**
- Error handling in all Python scripts
- Retry logic in `consumer/consumer.py`
- Resharding detection in consumer

**Exam Topics Covered:**
- Handle stream resharding
- Implement retry strategies
- Design fault-tolerant systems

### Domain 4: Data Security and Governance (18% of exam)

#### 4.1 Apply authentication mechanisms

**Demonstrated Skills:**
- ✅ **IAM Roles**: Dedicated roles for producer and consumer
- ✅ **Least Privilege**: Minimal permissions per component
- ✅ **Resource-Level Permissions**: Scoped to specific stream and table

**Lab Components:**
- IAM roles and policies in `terraform/main.tf`

**Exam Topics Covered:**
- Design IAM policies for data services
- Implement least privilege access
- Use resource-level permissions

#### 4.2 Apply authorization mechanisms

**Demonstrated Skills:**
- ✅ **Policy-Based Access**: IAM policies control access
- ✅ **Conditional Writes**: DynamoDB conditions prevent unauthorized updates
- ✅ **Separation of Duties**: Producer and consumer have different permissions

**Lab Components:**
- IAM policies in Terraform
- Conditional writes in consumer

#### 4.3 Apply encryption

**Demonstrated Skills:**
- ✅ **Encryption at Rest**: DynamoDB and Kinesis use AWS-managed keys
- ✅ **Encryption in Transit**: All AWS API calls use HTTPS

**Note**: The lab uses default AWS-managed encryption. For production, consider:
- Customer-managed KMS keys
- Field-level encryption for sensitive data

#### 4.4 Prepare data for compliance

**Demonstrated Skills:**
- ✅ **Data Retention**: Configurable retention periods
- ✅ **Audit Trail**: Processing metadata tracked
- ✅ **Data Deletion**: TTL for automated cleanup

**Lab Components:**
- Retention settings in Terraform
- Audit fields in DynamoDB records

## Key Exam Patterns Demonstrated

### Pattern 1: Exactly-Once Processing

**Exam Question Type**: "How do you ensure records are processed exactly once in a streaming application?"

**Lab Implementation**:
```python
# Check-and-Set Pattern
table.put_item(
    Item={'record_id': uuid, ...},
    ConditionExpression='attribute_not_exists(record_id)'
)
```

**Why This Matters**: 
- Common in real-world scenarios
- Frequently tested on exam
- Demonstrates understanding of at-least-once vs exactly-once

### Pattern 2: Idempotent Operations

**Exam Question Type**: "What design pattern ensures safe retries in data pipelines?"

**Lab Implementation**:
- UUIDs for record identification
- DynamoDB conditional writes
- Stateless processing logic

**Why This Matters**:
- Critical for resilient systems
- Required for AWS Well-Architected Framework
- Foundation for event-driven architectures

### Pattern 3: Stream Resharding

**Exam Question Type**: "How does your application handle Kinesis stream scaling?"

**Lab Implementation**:
- Consumer detects closed shards
- Automatically discovers new shards
- Idempotency prevents duplicate processing across resharding

**Why This Matters**:
- Common operational scenario
- Tests understanding of stream internals
- Required for production-grade applications

### Pattern 4: Error Handling

**Exam Question Type**: "How do you handle throttling in DynamoDB?"

**Lab Implementation**:
```python
# Exponential Backoff
for attempt in range(max_retries):
    try:
        # Operation
    except ThrottlingException:
        wait_time = (2 ** attempt) * 0.1
        time.sleep(wait_time)
```

**Why This Matters**:
- Best practice for AWS services
- Prevents cascade failures
- Optimizes resource utilization

## Sample Exam Questions (Based on Lab)

### Question 1: Ingestion
**Q**: Your application needs to ingest 1000 events/second with the ability to replay events from the last 24 hours. Which service should you use?

**A**: Amazon Kinesis Data Streams
- ✅ Supports replay via retention period
- ✅ Handles high throughput
- ✅ Maintains event order per partition key

**Lab Reference**: `terraform/main.tf` - Kinesis configuration

### Question 2: Idempotency
**Q**: How can you prevent duplicate processing of records when a consumer restarts?

**A**: Use DynamoDB with conditional writes to track processed record IDs
- ✅ Atomic operation prevents race conditions
- ✅ Persists across consumer restarts
- ✅ Works with multiple consumer instances

**Lab Reference**: `consumer/consumer.py` - `IdempotencyChecker` class

### Question 3: Scaling
**Q**: What happens to your consumer application when a Kinesis shard splits?

**A**: The parent shard closes, two child shards open, and the consumer must:
1. Complete processing the parent shard
2. Discover the new child shards
3. Begin processing from both child shards

**Lab Reference**: `scripts/reshard.py` - Split operation

### Question 4: Security
**Q**: What is the principle of least privilege for a Kinesis consumer that writes to DynamoDB?

**A**: Grant only:
- `kinesis:GetRecords`, `kinesis:DescribeStream` on the specific stream
- `dynamodb:PutItem`, `dynamodb:GetItem` on the specific table
- NO wildcard resources or admin permissions

**Lab Reference**: `terraform/main.tf` - Consumer IAM policy

### Question 5: Cost Optimization
**Q**: How can you reduce storage costs for idempotency tracking in DynamoDB?

**A**: Enable TTL (Time To Live) to automatically delete old records
- ✅ No manual cleanup needed
- ✅ No additional cost
- ✅ Happens asynchronously

**Lab Reference**: `terraform/main.tf` - TTL configuration

## Study Recommendations

### To Master This Domain

1. **Hands-On Practice** (Most Important)
   - Deploy this lab in your AWS account
   - Run producer and consumer
   - Trigger resharding events
   - Observe behavior in CloudWatch

2. **Deep Dive Topics**
   - Kinesis shard iterator types (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER)
   - DynamoDB consistency models (eventual vs strong)
   - IAM policy evaluation logic
   - AWS service quotas and limits

3. **Read AWS Documentation**
   - [Kinesis Data Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/)
   - [DynamoDB Developer Guide](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/)
   - [AWS Well-Architected Framework - Reliability Pillar](https://docs.aws.amazon.com/wellarchitected/latest/reliability-pillar/)

4. **Practice Questions**
   - AWS official practice exams
   - Whitepapers on streaming architectures
   - Re-invent videos on data engineering

## Key Takeaways for the Exam

✅ **Idempotency is Critical**: Always design for safe retries

✅ **Choose the Right Service**: Kinesis for streams, DynamoDB for state, S3 for archives

✅ **On-Demand vs Provisioned**: Understand cost and performance tradeoffs

✅ **Security First**: Least privilege, encryption, audit trails

✅ **Operational Excellence**: Monitoring, logging, automated responses

✅ **Cost Optimization**: TTL, right-sizing, lifecycle policies

## Additional Study Materials

- **AWS Skill Builder**: DEA-C01 exam prep course
- **AWS Workshops**: [Data Engineering Immersion Day](https://catalog.us-east-1.prod.workshops.aws/workshops/...)
- **AWS Blogs**: Search for "Kinesis idempotency", "stream processing patterns"
- **GitHub**: Explore AWS samples for real-world patterns

## How This Lab Helps You Pass

This lab provides:
1. **Hands-on experience** with key services (60% of exam success)
2. **Pattern recognition** for common scenarios (30% of exam success)
3. **Architectural thinking** for design questions (10% of exam success)

**Pro Tip**: On the exam, if you see keywords like "duplicate records", "retry", or "exactly-once", think of this lab's Check-and-Set pattern!

Good luck with your DEA-C01 certification! 🚀
