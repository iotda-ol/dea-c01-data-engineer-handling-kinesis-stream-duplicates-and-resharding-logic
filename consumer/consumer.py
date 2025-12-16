#!/usr/bin/env python3
"""
Kinesis Consumer with Idempotency (Check-and-Set Pattern)

This consumer processes inventory records from Kinesis using the KCL pattern.
It implements a "Check-and-Set" pattern with DynamoDB to ensure exactly-once
processing semantics, even with duplicates from producer retries or shard resharding.

DEA-C01 Alignment: Idempotent consumer patterns and handling duplicate records.
"""

import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IdempotencyChecker:
    """
    Manages idempotency checking using DynamoDB with Check-and-Set pattern.
    
    This class ensures that each record is processed exactly once by:
    1. Checking if the record_id exists in DynamoDB
    2. If not exists, atomically inserting it with a conditional write
    3. If exists, skipping processing
    """
    
    def __init__(self, table_name: str, region_name: str = 'us-east-1', ttl_days: int = 7):
        """
        Initialize the idempotency checker.
        
        Args:
            table_name: Name of the DynamoDB table
            region_name: AWS region
            ttl_days: Number of days to keep processed records
        """
        self.table_name = table_name
        self.ttl_days = ttl_days
        self.dynamodb = boto3.resource('dynamodb', region_name=region_name)
        self.table = self.dynamodb.Table(table_name)
        self.records_processed = 0
        self.duplicates_detected = 0
        
        logger.info(f"Initialized idempotency checker with table: {table_name}")
    
    def is_duplicate(self, record_id: str, shard_id: str, sequence_number: str) -> bool:
        """
        Check if a record has already been processed using Check-and-Set pattern.
        
        Args:
            record_id: Unique identifier of the record
            shard_id: Kinesis shard ID
            sequence_number: Kinesis sequence number
        
        Returns:
            True if duplicate (already processed), False if new
        """
        try:
            # Calculate TTL (Time To Live) for automatic cleanup
            ttl_timestamp = int((datetime.utcnow() + timedelta(days=self.ttl_days)).timestamp())
            
            # Try to insert the record with a conditional write
            # This will fail if the record_id already exists (duplicate)
            self.table.put_item(
                Item={
                    'record_id': record_id,
                    'shard_id': shard_id,
                    'sequence_number': sequence_number,
                    'processed_at': datetime.utcnow().isoformat(),
                    'ttl': ttl_timestamp
                },
                ConditionExpression='attribute_not_exists(record_id)'
            )
            
            # If we reach here, the insert succeeded - it's a new record
            self.records_processed += 1
            logger.info(f"Record {record_id} is NEW - processing")
            return False
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                # Record already exists - it's a duplicate
                self.duplicates_detected += 1
                logger.warning(f"Record {record_id} is DUPLICATE - skipping")
                return True
            else:
                # Other error - log and treat as new to avoid data loss
                logger.error(f"Error checking idempotency for {record_id}: {e}")
                return False
    
    def get_statistics(self) -> Dict[str, int]:
        """Get processing statistics."""
        return {
            'records_processed': self.records_processed,
            'duplicates_detected': self.duplicates_detected
        }


class InventoryProcessor:
    """Processes inventory records with business logic."""
    
    def __init__(self):
        self.total_processed = 0
        self.inventory_updates = {}  # In-memory inventory (example)
    
    def process_record(self, record_data: Dict[str, Any]) -> bool:
        """
        Process a single inventory record.
        
        Args:
            record_data: Parsed record data
        
        Returns:
            True if successful, False otherwise
        """
        try:
            record_id = record_data.get('record_id')
            product_id = record_data.get('product_id')
            quantity = record_data.get('quantity')
            warehouse = record_data.get('warehouse')
            
            logger.info(
                f"Processing record {record_id}: "
                f"Product={product_id}, Quantity={quantity}, Warehouse={warehouse}"
            )
            
            # Business logic: Update inventory
            key = f"{product_id}:{warehouse}"
            if key not in self.inventory_updates:
                self.inventory_updates[key] = 0
            self.inventory_updates[key] += quantity
            
            self.total_processed += 1
            
            logger.info(f"Successfully processed record {record_id}")
            logger.info(f"Current inventory for {key}: {self.inventory_updates[key]}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error processing record: {e}")
            return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get processing statistics."""
        return {
            'total_processed': self.total_processed,
            'inventory_state': self.inventory_updates
        }


class KinesisConsumer:
    """
    Kinesis consumer with idempotent processing using Check-and-Set pattern.
    
    This consumer:
    1. Reads records from Kinesis
    2. Checks DynamoDB for duplicates using atomic conditional writes
    3. Processes only new records
    4. Handles shard splits and merges gracefully
    """
    
    def __init__(
        self,
        stream_name: str,
        idempotency_table_name: str,
        region_name: str = 'us-east-1',
        app_name: str = 'inventory-consumer'
    ):
        """
        Initialize the Kinesis consumer.
        
        Args:
            stream_name: Name of the Kinesis stream
            idempotency_table_name: Name of the DynamoDB idempotency table
            region_name: AWS region
            app_name: Application name for tracking
        """
        self.stream_name = stream_name
        self.app_name = app_name
        self.region_name = region_name
        
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        self.idempotency_checker = IdempotencyChecker(idempotency_table_name, region_name)
        self.processor = InventoryProcessor()
        
        logger.info(f"Initialized Kinesis consumer for stream: {stream_name}")
    
    def get_shard_iterator(self, shard_id: str, iterator_type: str = 'TRIM_HORIZON') -> str:
        """
        Get a shard iterator for reading from a shard.
        
        Args:
            shard_id: ID of the shard
            iterator_type: Type of iterator (TRIM_HORIZON, LATEST, etc.)
        
        Returns:
            Shard iterator token
        """
        response = self.kinesis_client.get_shard_iterator(
            StreamName=self.stream_name,
            ShardId=shard_id,
            ShardIteratorType=iterator_type
        )
        return response['ShardIterator']
    
    def get_active_shards(self) -> List[Dict[str, Any]]:
        """
        Get list of active shards in the stream.
        
        Returns:
            List of shard information dictionaries
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            stream_description = response['StreamDescription']
            
            # Filter only active shards (not closed due to resharding)
            active_shards = [
                shard for shard in stream_description['Shards']
                if 'EndingSequenceNumber' not in shard.get('SequenceNumberRange', {})
            ]
            
            logger.info(f"Found {len(active_shards)} active shard(s)")
            return active_shards
            
        except ClientError as e:
            logger.error(f"Error describing stream: {e}")
            return []
    
    def process_records(self, records: List[Dict[str, Any]], shard_id: str) -> int:
        """
        Process a batch of records with idempotency checking.
        
        Args:
            records: List of Kinesis records
            shard_id: ID of the shard
        
        Returns:
            Number of successfully processed records
        """
        processed_count = 0
        
        for record in records:
            try:
                # Decode the record data
                data = json.loads(record['Data'].decode('utf-8'))
                record_id = data.get('record_id')
                sequence_number = record['SequenceNumber']
                
                logger.info(f"Received record {record_id} from shard {shard_id}")
                
                # Check-and-Set: Check if duplicate before processing
                if self.idempotency_checker.is_duplicate(record_id, shard_id, sequence_number):
                    logger.info(f"Skipping duplicate record {record_id}")
                    continue
                
                # Process the record (business logic)
                if self.processor.process_record(data):
                    processed_count += 1
                    
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON in record: {e}")
            except Exception as e:
                logger.error(f"Error processing record: {e}")
        
        return processed_count
    
    def consume_shard(self, shard_id: str, max_iterations: Optional[int] = None) -> int:
        """
        Consume records from a single shard.
        
        Args:
            shard_id: ID of the shard to consume
            max_iterations: Maximum number of GetRecords calls (None for unlimited)
        
        Returns:
            Total number of records processed
        """
        logger.info(f"Starting to consume shard: {shard_id}")
        
        shard_iterator = self.get_shard_iterator(shard_id)
        total_processed = 0
        iterations = 0
        
        while shard_iterator:
            if max_iterations and iterations >= max_iterations:
                logger.info(f"Reached max iterations ({max_iterations})")
                break
            
            try:
                response = self.kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                
                records = response.get('Records', [])
                if records:
                    logger.info(f"Retrieved {len(records)} record(s) from shard {shard_id}")
                    processed = self.process_records(records, shard_id)
                    total_processed += processed
                else:
                    logger.debug(f"No records available in shard {shard_id}")
                
                # Get next iterator
                shard_iterator = response.get('NextShardIterator')
                
                # Handle shard closure (due to resharding)
                if shard_iterator is None:
                    logger.info(f"Shard {shard_id} is closed (likely due to resharding)")
                    break
                
                iterations += 1
                
                # Rate limiting to avoid throttling
                if not records:
                    time.sleep(1)
                
            except ClientError as e:
                logger.error(f"Error getting records from shard {shard_id}: {e}")
                time.sleep(5)
        
        logger.info(f"Finished consuming shard {shard_id}: {total_processed} records processed")
        return total_processed
    
    def run(self, max_iterations: Optional[int] = None, single_shard: Optional[str] = None):
        """
        Run the consumer to process records from all active shards.
        
        Args:
            max_iterations: Maximum iterations per shard (None for unlimited)
            single_shard: Process only a specific shard (for testing)
        """
        logger.info(f"Starting Kinesis consumer: {self.app_name}")
        
        try:
            if single_shard:
                shards = [{'ShardId': single_shard}]
            else:
                shards = self.get_active_shards()
            
            if not shards:
                logger.warning("No active shards found")
                return
            
            # In production, use KCL for automatic shard assignment and checkpointing
            # This is a simplified version for demonstration
            for shard in shards:
                shard_id = shard['ShardId']
                self.consume_shard(shard_id, max_iterations)
            
            # Print statistics
            idempotency_stats = self.idempotency_checker.get_statistics()
            processor_stats = self.processor.get_statistics()
            
            logger.info("=== Consumer Statistics ===")
            logger.info(f"Records processed: {idempotency_stats['records_processed']}")
            logger.info(f"Duplicates detected: {idempotency_stats['duplicates_detected']}")
            logger.info(f"Total processed: {processor_stats['total_processed']}")
            logger.info(f"Final inventory state: {processor_stats['inventory_state']}")
            
        except KeyboardInterrupt:
            logger.info("\nConsumer stopped by user")
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)


def main():
    """Main function to run the consumer."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Kinesis Consumer with Idempotency'
    )
    parser.add_argument(
        '--stream-name',
        default='inventory-stream',
        help='Name of the Kinesis stream'
    )
    parser.add_argument(
        '--table-name',
        default='kinesis-idempotency-table',
        help='Name of the DynamoDB idempotency table'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region'
    )
    parser.add_argument(
        '--app-name',
        default='inventory-consumer',
        help='Application name'
    )
    parser.add_argument(
        '--max-iterations',
        type=int,
        help='Maximum iterations per shard (for testing)'
    )
    parser.add_argument(
        '--shard-id',
        help='Process only a specific shard (for testing)'
    )
    
    args = parser.parse_args()
    
    consumer = KinesisConsumer(
        stream_name=args.stream_name,
        idempotency_table_name=args.table_name,
        region_name=args.region,
        app_name=args.app_name
    )
    
    consumer.run(max_iterations=args.max_iterations, single_shard=args.shard_id)
    
    return 0


if __name__ == '__main__':
    exit(main())
