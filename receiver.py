import time
import json
import asyncio
import boto3
from botocore.client import BaseClient
from datetime import datetime

async def get_active_shards(kinesis_client: BaseClient, stream_name: str) -> list:
    """
    Retrieves the list of active shard IDs in an Amazon Kinesis Data Stream.

    Args:
        kinesis_client (BaseClient): The initialized boto3 Kinesis client.
        stream_name (str): The target Kinesis stream name.

    Returns:
        list: A list of shard ID strings.
    """
    response = await asyncio.to_thread(
        kinesis_client.list_shards,
        StreamName=stream_name
    )
    return [shard['ShardId'] for shard in response.get('Shards', [])]

async def receive_worker(
        kinesis_client: BaseClient, 
        stream_name: str
    ):
    """
    Asynchronously reads and consumes all available records continuously from all shards 
    in an Amazon Kinesis Data Stream until no more messages are left.

    Args:
        kinesis_client (BaseClient): The initialized boto3 Kinesis client.
        stream_name (str): The target Kinesis stream name.
    """
    start_time = time.perf_counter()
    start_str = datetime.now().strftime('%H:%M:%S')
    total_received = 0

    print(f"🔍 Looking for active shards in stream '{stream_name}'...")
    shard_ids = await get_active_shards(kinesis_client, stream_name)

    # Iterate over each shard in the stream
    for shard_id in shard_ids:
        print(f"📖 Reading from shard: {shard_id}...")
        
        # 1. Get the Shard Iterator
        # 'TRIM_HORIZON' starts reading from the oldest available data record in the shard.
        iterator_response = await asyncio.to_thread(
            kinesis_client.get_shard_iterator,
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType='TRIM_HORIZON' 
        )
        shard_iterator = iterator_response.get('ShardIterator')
        
        # 2. Continuous reading loop for the current shard iterator
        while shard_iterator:
            records_response = await asyncio.to_thread(
                kinesis_client.get_records,
                ShardIterator=shard_iterator,
                Limit=10000  # Max limit per get_records call
            )
            
            records = records_response.get('Records', [])
            
            for record in records:
                total_received += 1
                # Decode the binary Kinesis data to string
                body = record['Data'].decode("utf-8")
                try:
                    parsed_record = json.loads(body)
                    formatted_json = json.dumps(parsed_record, indent=2)
                    print(f"✔️ Received:\n{formatted_json}")
                except json.JSONDecodeError:
                    print(f"✔️ Received (Raw): {body}")
            
            # Update the iterator for the next batch of records
            shard_iterator = records_response.get('NextShardIterator')
            
            # If no records returned or iterator is missing, move to the next shard
            if not records:
                break
                
            # Optional small pause to prevent hitting read throughput limits too aggressively
            await asyncio.sleep(0.2)

    end_time = time.perf_counter()
    duration = end_time - start_time
        
    print(f"✅ Finished in {duration:.2f}s at {start_str} | {total_received:,} total rows received")

async def command(
        stream: str
    ):
    """
    Main command orchestrator to receive all available records from an Amazon Kinesis Data Stream.

    Args:
        stream (str): The target Kinesis stream name.
    """
    kinesis_client = boto3.client('kinesis')
    
    print(f"Starting to receive all messages from Kinesis Stream '{stream}'...")
    
    await receive_worker(kinesis_client, stream)
    
    print(f"Receiving from Kinesis finished successfully.")