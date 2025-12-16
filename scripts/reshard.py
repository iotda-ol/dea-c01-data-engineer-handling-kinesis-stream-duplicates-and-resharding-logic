#!/usr/bin/env python3
"""
Kinesis Stream Resharding Utility

This script provides utilities to split and merge shards in a Kinesis stream.
It helps demonstrate how the consumer handles resharding events gracefully
with the idempotency pattern.

DEA-C01 Alignment: Managing stream capacity and handling resharding scenarios.
"""

import argparse
import logging
import time
from typing import List, Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KinesisReshardManager:
    """
    Manages Kinesis stream resharding operations.
    
    Note: Resharding is only available for provisioned mode streams.
    On-Demand mode streams automatically scale.
    """
    
    def __init__(self, stream_name: str, region_name: str = 'us-east-1'):
        """
        Initialize the resharding manager.
        
        Args:
            stream_name: Name of the Kinesis stream
            region_name: AWS region
        """
        self.stream_name = stream_name
        self.kinesis_client = boto3.client('kinesis', region_name=region_name)
        logger.info(f"Initialized resharding manager for stream: {stream_name}")
    
    def get_stream_info(self) -> Dict[str, Any]:
        """
        Get detailed information about the stream.
        
        Returns:
            Stream description dictionary
        """
        try:
            response = self.kinesis_client.describe_stream(StreamName=self.stream_name)
            return response['StreamDescription']
        except ClientError as e:
            logger.error(f"Error describing stream: {e}")
            raise
    
    def list_shards(self) -> List[Dict[str, Any]]:
        """
        List all shards in the stream.
        
        Returns:
            List of shard dictionaries
        """
        try:
            response = self.kinesis_client.list_shards(StreamName=self.stream_name)
            shards = response['Shards']
            
            logger.info(f"Found {len(shards)} shard(s) in stream")
            
            # Categorize shards
            open_shards = [s for s in shards if 'EndingSequenceNumber' not in s['SequenceNumberRange']]
            closed_shards = [s for s in shards if 'EndingSequenceNumber' in s['SequenceNumberRange']]
            
            logger.info(f"Open shards: {len(open_shards)}, Closed shards: {len(closed_shards)}")
            
            return shards
        except ClientError as e:
            logger.error(f"Error listing shards: {e}")
            raise
    
    def print_shard_info(self):
        """Print detailed information about all shards."""
        shards = self.list_shards()
        
        print("\n" + "="*80)
        print(f"SHARD INFORMATION FOR STREAM: {self.stream_name}")
        print("="*80)
        
        for shard in shards:
            shard_id = shard['ShardId']
            hash_key_range = shard['HashKeyRange']
            seq_range = shard['SequenceNumberRange']
            
            status = "CLOSED" if 'EndingSequenceNumber' in seq_range else "OPEN"
            
            print(f"\nShard ID: {shard_id}")
            print(f"  Status: {status}")
            print(f"  Hash Key Range: {hash_key_range['StartingHashKey']} - {hash_key_range['EndingHashKey']}")
            print(f"  Starting Sequence: {seq_range['StartingSequenceNumber']}")
            if 'EndingSequenceNumber' in seq_range:
                print(f"  Ending Sequence: {seq_range['EndingSequenceNumber']}")
            
            # Show parent/child relationships
            if 'ParentShardId' in shard:
                print(f"  Parent Shard: {shard['ParentShardId']}")
            if 'AdjacentParentShardId' in shard:
                print(f"  Adjacent Parent: {shard['AdjacentParentShardId']}")
        
        print("\n" + "="*80 + "\n")
    
    def split_shard(self, shard_id: str, new_starting_hash_key: Optional[str] = None) -> bool:
        """
        Split a shard into two child shards.
        
        Args:
            shard_id: ID of the shard to split
            new_starting_hash_key: Starting hash key for the second child shard
                                  (if None, splits at midpoint)
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Get shard information
            shards = self.list_shards()
            target_shard = next((s for s in shards if s['ShardId'] == shard_id), None)
            
            if not target_shard:
                logger.error(f"Shard {shard_id} not found")
                return False
            
            # Check if shard is already closed
            if 'EndingSequenceNumber' in target_shard['SequenceNumberRange']:
                logger.error(f"Cannot split closed shard {shard_id}")
                return False
            
            # Calculate split point if not provided
            if new_starting_hash_key is None:
                start_key = int(target_shard['HashKeyRange']['StartingHashKey'])
                end_key = int(target_shard['HashKeyRange']['EndingHashKey'])
                new_starting_hash_key = str((start_key + end_key) // 2)
            
            logger.info(f"Splitting shard {shard_id} at hash key {new_starting_hash_key}")
            
            self.kinesis_client.split_shard(
                StreamName=self.stream_name,
                ShardToSplit=shard_id,
                NewStartingHashKey=new_starting_hash_key
            )
            
            logger.info(f"Successfully initiated split of shard {shard_id}")
            logger.info("Waiting for split to complete...")
            
            # Wait for split to complete
            self.wait_for_stream_active()
            
            logger.info("Split completed successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Error splitting shard: {e}")
            return False
    
    def merge_shards(self, shard_id: str, adjacent_shard_id: str) -> bool:
        """
        Merge two adjacent shards into a single shard.
        
        Args:
            shard_id: ID of the first shard to merge
            adjacent_shard_id: ID of the adjacent shard to merge
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Verify both shards exist and are open
            shards = self.list_shards()
            shard1 = next((s for s in shards if s['ShardId'] == shard_id), None)
            shard2 = next((s for s in shards if s['ShardId'] == adjacent_shard_id), None)
            
            if not shard1 or not shard2:
                logger.error("One or both shards not found")
                return False
            
            if 'EndingSequenceNumber' in shard1['SequenceNumberRange']:
                logger.error(f"Shard {shard_id} is already closed")
                return False
            
            if 'EndingSequenceNumber' in shard2['SequenceNumberRange']:
                logger.error(f"Shard {adjacent_shard_id} is already closed")
                return False
            
            logger.info(f"Merging shards {shard_id} and {adjacent_shard_id}")
            
            self.kinesis_client.merge_shards(
                StreamName=self.stream_name,
                ShardToMerge=shard_id,
                AdjacentShardToMerge=adjacent_shard_id
            )
            
            logger.info("Successfully initiated merge")
            logger.info("Waiting for merge to complete...")
            
            # Wait for merge to complete
            self.wait_for_stream_active()
            
            logger.info("Merge completed successfully")
            return True
            
        except ClientError as e:
            logger.error(f"Error merging shards: {e}")
            return False
    
    def wait_for_stream_active(self, max_wait_seconds: int = 300):
        """
        Wait for stream to become active after resharding.
        
        Args:
            max_wait_seconds: Maximum time to wait in seconds
        """
        start_time = time.time()
        
        while True:
            if time.time() - start_time > max_wait_seconds:
                logger.warning("Timeout waiting for stream to become active")
                break
            
            try:
                stream_info = self.get_stream_info()
                status = stream_info['StreamStatus']
                
                logger.info(f"Stream status: {status}")
                
                if status == 'ACTIVE':
                    logger.info("Stream is active")
                    break
                
                time.sleep(10)
                
            except ClientError as e:
                logger.error(f"Error checking stream status: {e}")
                time.sleep(10)
    
    def update_shard_count(self, target_shard_count: int) -> bool:
        """
        Update the shard count for the stream (for provisioned mode).
        
        Args:
            target_shard_count: Desired number of shards
        
        Returns:
            True if successful, False otherwise
        """
        try:
            logger.info(f"Updating shard count to {target_shard_count}")
            
            self.kinesis_client.update_shard_count(
                StreamName=self.stream_name,
                TargetShardCount=target_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            logger.info("Successfully initiated shard count update")
            logger.info("Waiting for update to complete...")
            
            self.wait_for_stream_active()
            
            logger.info("Shard count update completed")
            return True
            
        except ClientError as e:
            if 'ValidationException' in str(e):
                logger.error("Stream may be in ON_DEMAND mode. Shard count updates are not available for ON_DEMAND streams.")
            else:
                logger.error(f"Error updating shard count: {e}")
            return False


def main():
    """Main function for the resharding utility."""
    parser = argparse.ArgumentParser(
        description='Kinesis Stream Resharding Utility'
    )
    parser.add_argument(
        '--stream-name',
        required=True,
        help='Name of the Kinesis stream'
    )
    parser.add_argument(
        '--region',
        default='us-east-1',
        help='AWS region'
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Resharding command')
    
    # Info command
    subparsers.add_parser('info', help='Display shard information')
    
    # Split command
    split_parser = subparsers.add_parser('split', help='Split a shard')
    split_parser.add_argument('--shard-id', required=True, help='Shard ID to split')
    split_parser.add_argument('--hash-key', help='New starting hash key for split')
    
    # Merge command
    merge_parser = subparsers.add_parser('merge', help='Merge two adjacent shards')
    merge_parser.add_argument('--shard-id', required=True, help='First shard ID')
    merge_parser.add_argument('--adjacent-shard-id', required=True, help='Adjacent shard ID')
    
    # Update shard count command
    count_parser = subparsers.add_parser('update-count', help='Update shard count (provisioned mode only)')
    count_parser.add_argument('--target-count', type=int, required=True, help='Target shard count')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    # Create resharding manager
    manager = KinesisReshardManager(args.stream_name, args.region)
    
    # Execute command
    if args.command == 'info':
        manager.print_shard_info()
    
    elif args.command == 'split':
        success = manager.split_shard(args.shard_id, args.hash_key)
        if success:
            logger.info("Split operation completed successfully")
            manager.print_shard_info()
        else:
            logger.error("Split operation failed")
            return 1
    
    elif args.command == 'merge':
        success = manager.merge_shards(args.shard_id, args.adjacent_shard_id)
        if success:
            logger.info("Merge operation completed successfully")
            manager.print_shard_info()
        else:
            logger.error("Merge operation failed")
            return 1
    
    elif args.command == 'update-count':
        success = manager.update_shard_count(args.target_count)
        if success:
            logger.info("Shard count update completed successfully")
            manager.print_shard_info()
        else:
            logger.error("Shard count update failed")
            return 1
    
    return 0


if __name__ == '__main__':
    exit(main())
