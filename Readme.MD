# Amazon Kinesis Utility

A high-performance Python utility designed to benchmark, execute parallel batch data uploads, and stream/read records from Amazon Kinesis Data Streams.

---

## 🚀 Features
* **Parallel Batching (`send`):** Sends multiple record batches concurrently using `asyncio` and random batch sizing for realistic stress testing via `put_records`.
* **Multi-Shard Consumption (`receive`):** Automatically discovers active shards, reads all available historical and incoming data starting from `TRIM_HORIZON`, formats output cleanly as JSON, and streams until the stream is completely drained.
* **Async Core:** Built on `asyncio` and `boto3` (offloading blocking calls via `asyncio.to_thread`) for maximum I/O throughput.

---

## 🛠 Prerequisites
* Python 3.11+
* AWS CLI configured (or appropriate credentials set via environment variables/IAM role)
* Active Amazon Kinesis Data Stream

---

## 📋 Usage

### 1. Installation
```bash
pip install -r requirements.txt
```

### 2. Running the Commands
The tool is invoked via `utils.py` using two main commands:

#### A. Send Events in Parallel Batches (send)
Sends multiple batches of mock records in parallel to the Kinesis Stream, randomizing the size of each batch and displaying metrics upon completion.

* `--stream`: The target Amazon Kinesis Stream name.
* `--batches`: Number of concurrent batches to send.

```bash
python utils.py send --stream kds001 --batches 5
Starting uploading 5 batches in parallel to Kinesis Stream 'kds001'...
✅ [Batch #1] Finished in 0.92s at 17:27:35 | 13 rows sent
✅ [Batch #4] Finished in 0.90s at 17:27:35 | 6 rows sent
✅ [Batch #2] Finished in 0.91s at 17:27:35 | 12 rows sent
✅ [Batch #5] Finished in 0.90s at 17:27:35 | 11 rows sent
✅ [Batch #3] Finished in 0.91s at 17:27:35 | 5 rows sent
All batch uploads to Kinesis finished successfully.
```

#### B. Receive and Read Records (receive)
Discovers active shards, reads records continuously from TRIM_HORIZON, formats payloads cleanly as JSON, and finishes automatically when all shards are drained.

* `--stream`: The target Amazon Kinesis Stream name.

```bash
python utils.py receive --stream kds001
Starting to receive all messages from Kinesis Stream 'kds001'...
🔍 Looking for active shards in stream 'kds001'...
📖 Reading from shard: shardId-000000000000...
✔️ Received:
{
  "id": 664,
  "name": "Susan Hernandez",
  "username": "susher28",
  "email": "susan.hernandez@company.io",
  "address": {
    "street": "Jessica Light",
    "suite": "Apt. 944",
    "city": "Mooreborough",
    "zipcode": "27695-2069",
    "geo": {
      "lat": "-49.8919",
      "lng": "48.5902"
    }
  },
  "phone": "1-242-822-6374 x80655",
  "website": "hernandez65.org",
  "company": {
    "name": "Hernandez-Taylor",
    "catchPhrase": "Visionary maximizing middleware",
    "bs": "synergize scalable networks"
  }
}
...
✅ Finished in 4.09s at 17:37:30 | 47 total rows received
Receiving from Kinesis finished successfully.
```