It is completely understandable that the state-machine logic and the various moving parts could be confusing for a user running this for the first time. A comprehensive `README.md` is exactly what the handover team needs.

Here is a detailed, production-ready `README.md` that explains the architecture, the extraction modes, the density-aware chunking, and every single configuration parameter.

---

# MongoDB ETL Benchmark Engine (Prototype)

## Overview

This engine is a multiprocessing Python prototype designed to benchmark two distinct MongoDB extraction approaches (`DIRECT_STREAM` vs. `MONGODUMP_STAGE`) against the same source collection and destination cluster.

Its primary goal is to measure throughput, worker utilization, and end-to-end extraction time without putting undue stress on the source production databases. It utilizes a centralized bookkeeping database to manage atomic worker leasing, track metrics, and handle seamless job resumption.

---

## Architecture & How It Works

The system is built on **process-based parallelism** (using Python's `ProcessPoolExecutor`). To bypass Python's Global Interpreter Lock (GIL) and maximize EC2 core utilization, every worker process instantiates its own isolated `MongoClient`.

### 1. Extraction Modes

The engine supports two benchmark modes, selectable via the configuration file:

* **`DIRECT_STREAM`**: A single pool of Python workers queries the source database using indexed chunk boundaries. They stream documents into memory, pass them through a placeholder transform function, and execute unordered bulk upserts directly to the destination.
* **`MONGODUMP_STAGE`**: A two-phase conveyor belt.
* **Dump Workers:** Execute native `mongodump` subprocesses to rapidly extract chunk boundaries to local BSON files.
* **Load Workers:** Iteratively stream BSON files from disk (to prevent memory bloat), pass documents through the transform placeholder, and bulk upsert to the destination.
* *Note:* This mode enforces **backpressure**. Dump workers will pause if the staged raw data exceeds the `max_backlog_chunks` limit, preventing local disk exhaustion.



### 2. Bi-Directional Density-Aware Chunk Planner

To prevent "hot chunks" (OOM crashes) and "cold chunks" (wasted overhead), the engine avoids simple time-slicing. It uses an index-hinted planner that dynamically adjusts to the shape of your data:

* **Expansion Phase (The Trickle):** If a time window has fewer than 50% of the target documents, the planner doubles the time window (up to 24 hours) to gather a meaty payload.
* **Shrink Phase (The Spike):** If a time window exceeds the target document count (e.g., a massive bulk insert), the planner recursively cuts the time window in half until the chunk is safe for a single worker to handle.
* **Gap Compression:** It instantly fast-forwards over dead time periods (e.g., weekends with zero inserts) using `$gte` point queries.

### 3. State Management & Resumption

The `masking_control` database handles worker leasing via `find_one_and_update`.

* If a job is restarted with the **same `job_id**`, it instantly resumes. Interrupted/crashed chunks are reset to `READY`, while fully completed chunks are ignored.
* Chunks that fail repeatedly (exceeding `max_retries`) are quarantined in a `FAILED_*` state. To force a hard retry on these, simply change the `job_id` in the config or clear the bookkeeping database.

---

## Prerequisites & Installation

**System Requirements:**

* Python 3.8+
* MongoDB Native Tools (`mongodump` must be available in the system PATH).
* Sufficient local disk space for the staging directory if using `MONGODUMP_STAGE`.

**Python Dependencies:**

```bash
# WARNING: DO NOT run `pip install bson`. It will break the PyMongo BSON parser.
pip install pymongo>=4.0.0

```

---

## Configuration Guide (`config.json`)

The engine is controlled entirely via a JSON configuration file.

| Parameter | Type | Description |
| --- | --- | --- |
| **`job_id`** | String | Unique identifier for the benchmark. Changing this triggers a fresh run; keeping it triggers a resume of an interrupted run. |
| **`mode`** | String | Must be exactly `"DIRECT_STREAM"` or `"MONGODUMP_STAGE"`. |
| **`source_uri`** | String | MongoDB connection string for the source data. |
| **`dest_uri`** | String | MongoDB connection string for the target destination. |
| **`bookkeeping_uri`** | String | MongoDB connection string for the control database (`masking_control`). |
| **`source_db`** | String | Source database name. |
| **`source_collection`** | String | Source collection name. |
| **`dest_db`** | String | Destination database name. |
| **`dest_collection`** | String | Destination collection name. |
| **`staging_root`** | String | Absolute path to the local directory where Mongodump BSON files will be staged. |
| **`chunk_window_hours`** | Integer | The initial time-window guess for the planner (e.g., `1`). |
| **`max_docs_per_chunk`** | Integer | The maximum document threshold before the planner recursively splits the chunk (e.g., `100000`). |
| **`max_retries`** | Integer | How many times a worker will attempt a failed chunk before quarantining it. |
| **`src_batch_size`** | Integer | Cursor batch size for source reads (`DIRECT_STREAM` only). |
| **`dest_batch_size`** | Integer | The document count per unordered bulk write to the destination (e.g., `500`). |
| **`stream_workers`** | Integer | Number of parallel extract/load processes for `DIRECT_STREAM` mode. |
| **`dump_workers`** | Integer | Number of parallel Mongodump processes. |
| **`load_workers`** | Integer | Number of parallel BSON read/write processes. |
| **`max_backlog_chunks`** | Integer | Backpressure threshold. Dump workers pause if un-loaded staged chunks exceed this number. |

### Example `config.json`

```json
{
    "job_id": "benchmark_run_m10_vs_m30_01",
    "mode": "MONGODUMP_STAGE",
    "source_uri": "mongodb://source_user:pass@source_host:27017/",
    "dest_uri": "mongodb://dest_user:pass@dest_host:27017/",
    "bookkeeping_uri": "mongodb://localhost:27017/",
    "source_db": "production_data",
    "source_collection": "telemetry",
    "dest_db": "target_data",
    "dest_collection": "telemetry_clone",
    "staging_root": "/tmp/etl_staging",
    "chunk_window_hours": 1,
    "max_docs_per_chunk": 100000,
    "max_retries": 3,
    "src_batch_size": 2000,
    "dest_batch_size": 500,
    "stream_workers": 8,
    "dump_workers": 4,
    "load_workers": 4,
    "max_backlog_chunks": 10
}

```

---

## Execution Instructions

### 1. Test the Chunk Planner (Optional but Recommended)

Before kicking off a massive extraction, you can verify how the bi-directional planner will split your data. This creates the bookkeeping records without spinning up any worker pools.

```bash
python test_chunker.py --config config.json

```

*Review the output to ensure `Est. Doc Count` stays below your configured maximum, and verify that empty time periods are skipped.*

### 2. Run the Benchmark

Execute the main orchestrator. It will read the config, initialize the database, plan the chunks (if not already planned), and spin up the multiprocessing worker pools.

```bash
python etl_benchmark.py --config config.json

```

### 3. Review the Final Output

Upon completion, the script will output a benchmark summary to the console and save it to the `jobs` collection in the bookkeeping database. It includes:

* Total runtime seconds.
* Total documents written.
* Total bytes staged (for Mongodump mode).
* Effective documents per second.
* Effective MB per second.

### Development Note: The Transform Placeholder

Inside `etl_benchmark.py`, there is a function named `transform_document_placeholder(doc: dict) -> dict`. For the purposes of this benchmark, it returns the document unchanged. In the future, the masking and tokenization engine will be injected directly into this function.

Install Pip: 
sudo python3 -m ensurepip --default-pip
pip install pymongo
scp -i ~/.ssh/bsk-test.pem \
  etl_benchmark.py \
  test_chunker.py \
  config.json \
  ec2-user@ec2-3-81-234-182.compute-1.amazonaws.com:/home/ec2-user/
sudo yum install -y tmux

python3 -c "import secrets; print(secrets.token_hex(32))"
export FMDM_HASH_SALT="33a0f9ffa27b505ef7c2d2d44f2d5b0d49af41eb22e40562a8a60e59d05974cb"