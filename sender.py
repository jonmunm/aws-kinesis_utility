import time
import json
import uuid
import random
import asyncio
import boto3
from botocore.client import BaseClient
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
fake_user_file = f"{BASE_DIR}/users_fake_data.json"

async def send_worker(
        kinesis_client: BaseClient,
        stream_name: str,
        batch_id: int,
        all_records: list
    ):
    """
    Asynchronously creates and sends a single batch of randomly selected mock records 
    to an Amazon Kinesis Data Stream using put_records.

    Args:
        kinesis_client (BaseClient): The initialized boto3 Kinesis client.
        stream_name (str): The target Kinesis stream name.
        batch_id (int): Identifier number for the current batch.
        all_records (list): The complete list of source records loaded from the mock file.
    """
    start_time = time.perf_counter()
    start_str = datetime.now().strftime('%H:%M:%S')

    # Define a random batch size (e.g., between 5 and 15 records per batch)
    # Ensuring we do not exceed the total records available in the file
    max_limit = min(15, len(all_records))
    min_limit = min(5, max_limit)
    batch_size = random.randint(min_limit, max_limit) if max_limit >= min_limit else len(all_records)
    
    # Select a random subset of records for this batch
    chunk = random.sample(all_records, batch_size)
    row_count = len(chunk)
    
    kinesis_records = []
    for record in chunk:
        kinesis_records.append({
            'Data': json.dumps(record).encode('utf-8'),
            'PartitionKey': str(uuid.uuid4())
        })
    
    if kinesis_records:
        # Offload blocking boto3 call to a separate thread for async execution
        response = await asyncio.to_thread(
            kinesis_client.put_records,
            StreamName=stream_name,
            Records=kinesis_records
        )
        
        failed_count = response.get('FailedRecordCount', 0)
        if failed_count > 0:
            print(f"⚠️ [Batch #{batch_id}] {failed_count} records failed to upload.")

    end_time = time.perf_counter()
    duration = end_time - start_time
        
    print(f"✅ [Batch #{batch_id}] Finished in {duration:.2f}s at {start_str} | {row_count:,} rows sent")


async def command(
        stream: str, 
        batches: int
    ):
    """
    Main command orchestrator to upload multiple batches of mock data in parallel 
    to an Amazon Kinesis Data Stream.

    Args:
        stream (str): The target Kinesis stream name.
        batches (int): Total number of concurrent batches to send.
    """
    kinesis_client = boto3.client('kinesis')
    
    # Load base records only once
    with open(fake_user_file, "r", encoding="utf-8") as archivo:
        records = json.load(archivo)

    print(f"Starting uploading {batches} batches in parallel to Kinesis Stream '{stream}'...")
    
    # Create concurrent tasks for each requested batch
    tasks = [
        send_worker(kinesis_client, stream, i + 1, records) 
        for i in range(batches)
    ]
    
    # Execute all batches in parallel
    await asyncio.gather(*tasks)
    
    print(f"All batch uploads to Kinesis finished successfully.")