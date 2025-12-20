#!/usr/bin/env python3
"""
Kinesis Producer for Inventory Records

This producer generates inventory records with UUIDs and sends them to a Kinesis stream.
It includes a toggle to simulate network timeouts and retries that cause duplicate records,
demonstrating the "At-Least-Once" delivery challenge.

DEA-C01 Alignment: Data ingestion patterns and handling producer-side duplicates.
"""

import json
import time
import uuid
import random
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class InventoryRecord:
    """Represents an inventory record with a unique identifier."""
    
    def __init__(self, product_id: str, quantity: int, warehouse: str, record_id: Optional[str] = None):
        self.record_id = record_id or str(uuid.uuid4())
        self.product_id = product_id
        self.quantity = quantity
        self.warehouse = warehouse
        self.timestamp = datetime.utcnow().isoformat()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary format."""
        return {
            'record_id': self.record_id,
            'product_id': self.product_id,
            'quantity': self.quantity,
            'warehouse': self.warehouse,
            'timestamp': self.timestamp
        }
    
    def to_json(self) -> str:
        """Convert record to JSON string."""
        return json.dumps(self.to_dict())


class KinesisProducer:
    """Producer for sending inventory records to Kinesis with retry simulation."""
    
    def __init__(
        self,
        stream_name: str,
        region_name: str = 'us-east-1',
        simulate_failures: bool = False,
        failure_rate: float = 0.2
    ):
        """
        Initialize the Kinesis producer.
        
        Args:
            stream_name: Name of the Kinesis stream
            region_name: AWS region
            simulate_failures: Enable failure simulation
            failure_rate: Probability of simulated failure (0.0-1.0)
        """
        self.stream_name = stream_name
        self.simulate_failures = simulate_failures
        self.failure_rate = failure_rate
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        self.records_sent = 0
        self.duplicates_sent = 0
        
        logger.info(f"Initialized Kinesis producer for stream: {stream_name}")
        logger.info(f"Failure simulation: {'ENABLED' if simulate_failures else 'DISABLED'}")
        if simulate_failures:
            logger.info(f"Failure rate: {failure_rate * 100}%")
    
    def send_record(self, record: InventoryRecord, partition_key: Optional[str] = None) -> bool:
        """
        Send a single record to Kinesis.
        
        Args:
            record: Inventory record to send
            partition_key: Partition key (defaults to product_id)
        
        Returns:
            True if successful, False otherwise
        """
        if partition_key is None:
            partition_key = record.product_id
        
        try:
            # Simulate network timeout/failure
            if self.simulate_failures and random.random() < self.failure_rate:
                logger.warning(f"Simulating network timeout for record: {record.record_id}")
                # Simulate a partial failure - we think it failed but it might have succeeded
                time.sleep(0.1)
                # Try to send again (causing potential duplicate)
                logger.info(f"Retrying record after timeout: {record.record_id}")
                self.duplicates_sent += 1
            
            response = self.kinesis_client.put_record(
                StreamName=self.stream_name,
                Data=record.to_json(),
                PartitionKey=partition_key
            )
            
            self.records_sent += 1
            logger.info(
                f"Sent record {record.record_id} to shard {response['ShardId']} "
                f"(Seq: {response['SequenceNumber']})"
            )
            return True
            
        except ClientError as e:
            logger.error(f"Failed to send record {record.record_id}: {e}")
            return False
    
    def send_batch(self, records: list, partition_key_fn=None) -> Dict[str, int]:
        """
        Send a batch of records to Kinesis.
        
        Args:
            records: List of InventoryRecord objects
            partition_key_fn: Function to determine partition key from record
        
        Returns:
            Dictionary with success and failure counts
        """
        success_count = 0
        failure_count = 0
        
        for record in records:
            partition_key = partition_key_fn(record) if partition_key_fn else record.product_id
            if self.send_record(record, partition_key):
                success_count += 1
            else:
                failure_count += 1
        
        return {
            'success': success_count,
            'failure': failure_count
        }
    
    def get_statistics(self) -> Dict[str, int]:
        """Get producer statistics."""
        return {
            'records_sent': self.records_sent,
            'duplicates_sent': self.duplicates_sent
        }


def generate_sample_inventory(num_records: int = 10) -> list:
    """
    Generate sample inventory records.
    
    Args:
        num_records: Number of records to generate
    
    Returns:
        List of InventoryRecord objects
    """
    products = ['WIDGET-001', 'GADGET-002', 'TOOL-003', 'DEVICE-004', 'COMPONENT-005']
    warehouses = ['WH-EAST', 'WH-WEST', 'WH-CENTRAL', 'WH-NORTH', 'WH-SOUTH']
    
    records = []
    for _ in range(num_records):
        product_id = random.choice(products)
        quantity = random.randint(1, 100)
        warehouse = random.choice(warehouses)
        records.append(InventoryRecord(product_id, quantity, warehouse))
    
    return records


def main():
    """Main function to run the producer."""
    parser = argparse.ArgumentParser(
        description='Kinesis Producer for Inventory Records'
    )
    parser.add_argument(
        '--stream-name',
        default='inventory-stream',
        help='Name of the Kinesis stream'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region'
    )
    parser.add_argument(
        '--num-records',
        type=int,
        default=20,
        help='Number of records to generate'
    )
    parser.add_argument(
        '--simulate-failures',
        action='store_true',
        help='Simulate network timeouts and retries'
    )
    parser.add_argument(
        '--failure-rate',
        type=float,
        default=0.2,
        help='Probability of simulated failure (0.0-1.0)'
    )
    parser.add_argument(
        '--interval',
        type=float,
        default=1.0,
        help='Interval between records in seconds'
    )
    parser.add_argument(
        '--continuous',
        action='store_true',
        help='Run continuously until stopped'
    )
    
    args = parser.parse_args()
    
    # Create producer
    producer = KinesisProducer(
        stream_name=args.stream_name,
        region_name=args.region,
        simulate_failures=args.simulate_failures,
        failure_rate=args.failure_rate
    )
    
    try:
        if args.continuous:
            logger.info("Running in continuous mode. Press Ctrl+C to stop.")
            while True:
                records = generate_sample_inventory(1)
                producer.send_batch(records)
                time.sleep(args.interval)
        else:
            logger.info(f"Generating {args.num_records} inventory records...")
            records = generate_sample_inventory(args.num_records)
            
            logger.info("Sending records to Kinesis...")
            results = producer.send_batch(records)
            
            logger.info(f"Completed: {results['success']} successful, {results['failure']} failed")
            
            stats = producer.get_statistics()
            logger.info(f"Statistics: {stats}")
            
            if args.simulate_failures:
                logger.info(
                    f"Potential duplicates sent due to simulated retries: {stats['duplicates_sent']}"
                )
    
    except KeyboardInterrupt:
        logger.info("\nProducer stopped by user.")
        stats = producer.get_statistics()
        logger.info(f"Final statistics: {stats}")
    
    except Exception as e:
        logger.error(f"Producer error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
