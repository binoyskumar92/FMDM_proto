# FMDM Prototype — MongoDB Field-Level Masking ETL

A multiprocessing Python tool that copies MongoDB collections from a source cluster to a destination cluster while masking configured PII fields in transit. Uses a centralized bookkeeping database for chunk state management, atomic worker leasing, and job resumption.

> **Status:** Prototype. Auth currently uses connection string URIs — AWS IAM integration is planned.

---

## How It Works

### Extraction Modes

**`DIRECT_STREAM`** *(primary)*
Workers query the source using chunk boundaries, stream documents into memory, apply field masking, and bulk-write to the destination. Each worker holds its own `MongoClient`.

**`MONGODUMP_STAGE`** *(secondary)*
Two-phase pipeline: dump workers run native `mongodump` subprocesses to write BSON files to local disk, then load workers stream those files, apply masking, and write to the destination. Backpressure halts dump workers if unloaded staged chunks exceed `max_backlog_chunks`, preventing disk exhaustion.

### Chunk Planner

The planner divides each collection into safe, evenly-loaded chunks using ObjectId range boundaries rather than fixed time windows. It is bi-directional:

- **Expansion:** if a time window yields fewer than 50% of `max_docs_per_chunk`, it doubles the window (up to 24h) to avoid tiny chunks.
- **Shrink:** if a window exceeds `max_docs_per_chunk`, it binary-searches for a safe boundary. If documents are dense within a single second, it falls back to binary search on raw ObjectId byte space.
- **Gap compression:** empty time ranges are skipped instantly using a forward scan.

> **Requirement:** collections must use `ObjectId` for `_id`. Custom `_id` types are not currently supported.

### Masking

Fields listed under `masking_fields` are masked using HMAC-SHA256 with a secret salt. Masking is shape-preserving: digits map to digits, letters to letters (case-preserved), and symbols (dashes, spaces, `@`, etc.) are kept as-is. So `"123-45-6789"` becomes something like `"847-29-5163"`.

Masking is deterministic — the same input value always produces the same masked output for a given salt. This preserves referential integrity across documents and collections.

The salt is never stored. It must be provided at runtime via the `FMDM_HASH_SALT` environment variable (see Setup below).

### State Management & Resumption

State lives in a `masking_control` database on the bookkeeping cluster. Workers lease chunks atomically via `find_one_and_update`. If a job is restarted with the same `job_id`, it resumes — completed chunks are skipped, in-flight chunks from the crashed run are reset to `READY`. Chunks that fail more than `max_retries` times are quarantined in `FAILED_*` / `QUARANTINED_*` states and do not block the rest of the job.

To force a clean restart, change `job_id` in `config.json`.

---

## Prerequisites

- Python 3.8+
- `mongodump` in PATH (required for `MONGODUMP_STAGE` mode only)
- Access to three MongoDB clusters: source, destination, and bookkeeping (bookkeeping can share the destination cluster)

```bash
pip install -r requirements.txt
```

> **Do not** run `pip install bson` — it conflicts with PyMongo's bundled BSON parser.

---

## Setup

### 1. Copy and edit the config

```bash
cp config_sample.json config.json
```

Edit `config.json` with your connection strings, database names, collections, and masking fields. See the [Configuration Reference](#configuration-reference) below.

### 2. Set the masking salt

The salt must be a 64-character hex string (32 raw bytes). Generate one:

```bash
python3 -c "import secrets; print(secrets.token_hex(32))"
```

Set it as an environment variable before running. On EC2, pull it from AWS Secrets Manager and export it:

```bash
export FMDM_HASH_SALT="<your-64-char-hex-string>"
```

The script will refuse to start if this variable is not set.

---

## Running

### Step 1 — Validate the chunk plan (optional but recommended)

Runs the planner against your source collection and prints the chunk boundaries without moving any data. Useful for checking that chunk sizes look sane before a large job.

```bash
python test_chunker.py --config config.json
```

Check that `Est. Doc Count` stays below `max_docs_per_chunk` for all chunks, and that empty date ranges are skipped.

### Step 2 — Run the ETL

```bash
python etl_benchmark.py --config config.json
```

The script will initialize the bookkeeping DB, plan chunks (or resume if the job already exists), spin up worker pools, and print a summary on completion.

For long-running jobs on EC2, run inside `tmux` so the job survives SSH disconnects:

```bash
tmux new -s fmdm
python etl_benchmark.py --config config.json
# Ctrl+B then D to detach
```

### Output

On completion, a summary is printed to stdout and written to the `jobs` collection in the bookkeeping database:

```
total_runtime_seconds
total_docs_written
completed_chunks
effective_docs_per_second
effective_mb_per_second        # MONGODUMP_STAGE only
chunk_status_distribution      # breakdown of LOADED, FAILED, QUARANTINED, etc.
```

### Masking Audit

Each chunk record written to the bookkeeping DB includes a `field_mask_counts` field showing how many documents in that chunk had each masked field present:

```json
{ "ssn": 9841, "account.account_number": 9841, "first_name": 9823 }
```

To get a full coverage report across the entire job, run this against the bookkeeping cluster after the job completes. Update the `$group` stage with your actual field paths:

```js
db.chunks.aggregate([
  { $match: { job_id: "your_job_id", status: "LOADED" } },
  { $replaceRoot: { newRoot: "$field_mask_counts" } },
  { $group: {
      _id: null,
      "ssn":                    { $sum: "$ssn" },
      "account.account_number": { $sum: "$account.account_number" },
      "first_name":             { $sum: "$first_name" }
  }}
])
```

This tells you the total number of documents where each sensitive field was present and masked — without storing any PII.

---

## Configuration Reference

`config.json` controls the entire job. Use `config_sample.json` as a starting point.

| Parameter | Type | Description |
|---|---|---|
| `job_id` | string | Unique job identifier. Reuse to resume; change to start fresh. |
| `mode` | string | `"DIRECT_STREAM"` or `"MONGODUMP_STAGE"` |
| `source_uri` | string | Connection string for the source cluster |
| `dest_uri` | string | Connection string for the destination cluster |
| `bookkeeping_uri` | string | Connection string for the bookkeeping cluster (can be same as `dest_uri`) |
| `source_db` | string | Source database name |
| `dest_db` | string | Destination database name |
| `dest_write_mode` | string | `"UPSERT"` (safe, default) or `"INSERT_THEN_UPSERT_ON_DUP"` (faster on clean runs) |
| `staging_root` | string | Local path for BSON staging files (`MONGODUMP_STAGE` only) |
| `chunk_window_hours` | int | Initial planner time window guess. Start with `1`. |
| `max_docs_per_chunk` | int | Max documents per chunk before the planner splits. |
| `max_retries` | int | Retry attempts per chunk before quarantine. |
| `src_batch_size` | int | MongoDB cursor batch size for source reads (`DIRECT_STREAM` only) |
| `dest_batch_size` | int | Documents per bulk write to destination |
| `stream_workers` | int | Parallel workers for `DIRECT_STREAM` |
| `dump_workers` | int | Parallel mongodump processes for `MONGODUMP_STAGE` |
| `load_workers` | int | Parallel BSON load processes for `MONGODUMP_STAGE` |
| `max_backlog_chunks` | int | Backpressure limit for `MONGODUMP_STAGE` |
| `lease_hours` | float | How long a worker holds a chunk lease before it can be stolen by another worker |
| `collections` | array | List of collection configs — see below |

Each entry in `collections`:

| Parameter | Type | Description |
|---|---|---|
| `source_collection` | string | Source collection name |
| `dest_collection` | string | Destination collection name |
| `drop_destination_collection_before_load` | bool | Drop and recreate destination before loading. Also triggers index sync from source. Recommended: `true`. |
| `masking_fields` | array of strings | Dot-notation field paths to mask, e.g. `"host.host_name"`, `"account.ssn"`. Pass `[]` to migrate the collection without any masking. |

### Config examples

**With masking** — migrates a collection and masks specific PII fields:

```json
{
    "source_collection": "customers",
    "dest_collection": "customers",
    "drop_destination_collection_before_load": true,
    "masking_fields": [
        "ssn",
        "account.account_number",
        "contact.email"
    ]
}
```

**Without masking** — pure migration, no fields touched. Passing an empty array causes `transform_document` to return the document unchanged:

```json
{
    "source_collection": "reference_data",
    "dest_collection": "reference_data",
    "drop_destination_collection_before_load": true,
    "masking_fields": []
}
```

Both can be mixed freely in the same job under `collections`.

### Full config example

```json
{
    "job_id": "prod_to_nonprod_run_001",
    "mode": "DIRECT_STREAM",
    "source_uri": "mongodb+srv://<user>:<pass>@source-cluster.mongodb.net/",
    "dest_uri": "mongodb+srv://<user>:<pass>@dest-cluster.mongodb.net/",
    "bookkeeping_uri": "mongodb+srv://<user>:<pass>@dest-cluster.mongodb.net/",
    "source_db": "financial_data",
    "dest_db": "financial_data_nonprod",
    "dest_write_mode": "INSERT_THEN_UPSERT_ON_DUP",
    "staging_root": "/tmp/mongo_staging",
    "chunk_window_hours": 1,
    "max_docs_per_chunk": 100000,
    "max_retries": 3,
    "src_batch_size": 2000,
    "dest_batch_size": 1000,
    "stream_workers": 10,
    "dump_workers": 4,
    "load_workers": 4,
    "max_backlog_chunks": 10,
    "lease_hours": 1,
    "collections": [
        {
            "source_collection": "customers",
            "dest_collection": "customers",
            "drop_destination_collection_before_load": true,
            "masking_fields": [
                "ssn",
                "account.account_number",
                "contact.email",
                "contact.phone"
            ]
        },
        {
            "source_collection": "transactions",
            "dest_collection": "transactions",
            "drop_destination_collection_before_load": true,
            "masking_fields": [
                "card.card_number",
                "card.holder_name"
            ]
        },
        {
            "source_collection": "reference_data",
            "dest_collection": "reference_data",
            "drop_destination_collection_before_load": true,
            "masking_fields": []
        }
    ]
}
```

---

## Known Limitations

- `_id` must be `ObjectId`. Collections with custom `_id` types (UUID, string, int) are not supported.
- Only one masking technique is currently implemented: HMAC-SHA256 shape-preserving hash. Nullification and other strategies are not yet available.
- Auth is connection-string based. AWS IAM integration is planned.
- No masking coverage report — there is currently no per-field audit output confirming 100% of documents were masked.
