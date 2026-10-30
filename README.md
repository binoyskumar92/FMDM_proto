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

All collections use a universal index-walk planner that works with any `_id` type (ObjectId, UUID, string, int — anything sortable). It walks the `_id` index in steps of `max_docs_per_chunk`, pulling one boundary document per step. For very large collections a dynamic chunk size cap prevents planning from creating more than 20,000 chunks.

The planner runs once at job start. On resume it is skipped — existing chunks are reused.

> **Note:** `MONGODUMP_STAGE` is only supported for ObjectId `_id` collections. `DIRECT_STREAM` works for all `_id` types.

### Masking

Fields listed under `masking_fields` are masked using HMAC-SHA256 with a secret salt. Masking is shape-preserving: digits map to digits, letters to letters (case-preserved), symbols kept as-is. So `"123-45-6789"` becomes something like `"847-29-5163"`.

Masking is deterministic — the same input always produces the same output for a given salt. This preserves referential integrity across collections.

**Unmaskable fields:** if a field contains a type that cannot be masked (nested object at a scalar path, undecodable binary), the field is replaced with `"MASKED_UNSUPPORTED"` and the document is still written to the destination. A record is written to the `masking_control.dead_letters` collection for investigation. The ETL Wizard surfaces these in its substitution report.

The salt is never stored. It must be provided at runtime via the `FMDM_HASH_SALT` environment variable.

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

Edit `config.json` with your connection strings, database names, collections, and masking fields. See [Configuration Reference](#configuration-reference) below.

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

### Option A — ETL Wizard (recommended)

The wizard is an interactive terminal UI that wraps `etl_benchmark.py`. It handles config selection, connectivity checks, salt entry, live monitoring, and shows a full job summary and masking audit report on completion.

```bash
python etl_wizard.py
```

See [ETL Wizard](#etl-wizard) section below for details.

### Option B — Direct execution

#### Step 1 — Validate the chunk plan (optional but recommended)

Runs the planner and prints chunk boundaries without moving any data. Useful for checking chunk sizes before a large job.

```bash
python test_chunker.py --config config.json
```

#### Step 2 — Run the ETL

```bash
python etl_benchmark.py --config config.json
```

The script initializes the bookkeeping DB, plans chunks, spins up worker pools, and prints a summary on completion.

For long-running jobs on EC2, run inside `tmux`:

```bash
tmux new -s fmdm
python etl_benchmark.py --config config.json
# Ctrl+B then D to detach
```

---

## Configuration Reference

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
| `max_docs_per_chunk` | int | Max documents per chunk. Planner uses this as the step size when walking the index. |
| `max_retries` | int | Retry attempts per chunk before quarantine. |
| `src_batch_size` | int | MongoDB cursor batch size for source reads (`DIRECT_STREAM` only) |
| `dest_batch_size` | int | Documents per bulk write to destination |
| `stream_workers` | int | Parallel workers for `DIRECT_STREAM` |
| `dump_workers` | int | Parallel mongodump processes for `MONGODUMP_STAGE` |
| `load_workers` | int | Parallel BSON load processes for `MONGODUMP_STAGE` |
| `max_backlog_chunks` | int | Backpressure limit for `MONGODUMP_STAGE` |
| `lease_hours` | float | How long a worker holds a chunk lease before another worker can steal it |
| `collections` | array | List of collection configs — see below |

Each entry in `collections`:

| Parameter | Type | Description |
|---|---|---|
| `source_collection` | string | Source collection name |
| `dest_collection` | string | Destination collection name |
| `drop_destination_collection_before_load` | bool | Drop and recreate destination before loading. Also syncs indexes from source. Recommended: `true` for initial runs. |
| `masking_fields` | array of strings | Dot-notation field paths to mask, e.g. `"account.ssn"`. Pass `[]` to migrate without masking. |

### Full config example

```json
{
    "job_id": "prod_migration_001",
    "mode": "DIRECT_STREAM",
    "source_uri": "mongodb+srv://<user>:<pass>@source-cluster.mongodb.net/",
    "dest_uri": "mongodb+srv://<user>:<pass>@dest-cluster.mongodb.net/",
    "bookkeeping_uri": "mongodb+srv://<user>:<pass>@dest-cluster.mongodb.net/",
    "source_db": "financial_data",
    "dest_db": "financial_data_nonprod",
    "dest_write_mode": "INSERT_THEN_UPSERT_ON_DUP",
    "staging_root": "/tmp/mongo_staging",
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

## ETL Wizard

`etl_wizard.py` is an interactive terminal UI that wraps `etl_benchmark.py`.

```bash
python etl_wizard.py
```

Requires `questionary` and `rich`, both in `requirements.txt`.

### Menu options

**Launch / Resume a job** — picks up any `.json` config files in the current directory, lets you override the `job_id` inline (persists the change to the config file), checks if a job already exists in the bookkeeping DB and shows its current progress, pings all three clusters, prompts for the salt if `FMDM_HASH_SALT` is not already set, then launches the ETL and drops into live monitoring. On completion shows a job summary and masking audit report automatically.

**Monitor an existing job** — attaches a live status panel to a running job. Refreshes every 10 seconds. Shows planning progress, chunk progress, docs written, substitution count, in-progress/failed counts, and elapsed time. Ctrl+C exits the monitor without touching the ETL process.

**Stop a running job** — looks up the ETL process PID from the bookkeeping DB, confirms, then sends SIGTERM to the entire process group (ETL + all workers). Updates job status to `STOPPED_BY_USER`. The job can be resumed later using the same `job_id`.

**View job history** — lists the 10 most recent jobs with status, chunk count, docs written, and start time.

**Dry run** — pings all three clusters without launching anything.

### Live status panel

During a running job the monitor shows:

| Field | Description |
|---|---|
| Planning | `Planning chunks...` while the planner runs; `✓ N chunks in X.Xs → Loading data...` once done |
| Progress | `█████░░░░░ 450/3456 (13.0%)` — loaded chunks vs total |
| In Progress | Chunks currently held by workers |
| Remaining | Chunks waiting to be leased |
| Failed | Chunks in a FAILED state (will be retried) |
| Quarantined | Chunks that exhausted all retries |
| Docs Written | Total documents written to destination so far |
| Docs Substituted | Documents where one or more fields could not be masked and were set to `MASKED_UNSUPPORTED`. Yellow < 1,000; bold red ≥ 1,000 with a stop warning. |

### Job summary and masking audit

When a job completes via the wizard, two reports are shown automatically:

**Job Summary** — runtime, docs written, chunks completed, docs/sec, write mode breakdown. For `MONGODUMP_STAGE` also shows dump size and MB/sec.

**Masking Audit Report** — per-collection, per-field coverage showing how many documents had each masked field present, as a percentage of that collection's total docs written.

**Substitution Report** — only shown if any fields were replaced with `MASKED_UNSUPPORTED`. Shows affected collection, count, and up to 5 sample reasons with the offending document `_id`. Full records are in `masking_control.dead_letters`.

---

## Masking Audit (manual query)

Each chunk record in `masking_control.chunks` stores a `field_mask_counts` map. To query coverage directly from the bookkeeping cluster:

```js
db.chunks.aggregate([
  { $match: { job_id: "your_job_id", status: { $in: ["LOADED", "RAW_DELETED"] } } },
  { $group: {
      _id: "$source_collection",
      ssn:                    { $sum: "$field_mask_counts.ssn" },
      "account.account_number": { $sum: "$field_mask_counts.account\\.account_number" }
  }}
])
```

To query substituted documents:

```js
db.dead_letters.find({ job_id: "your_job_id" })
```

---

## Known Limitations

- `MONGODUMP_STAGE` requires ObjectId `_id`. Use `DIRECT_STREAM` for collections with UUID, string, or integer `_id` types.
- Only HMAC-SHA256 shape-preserving masking is implemented. Nullification and tokenization are not yet available.
- Auth is connection-string based. AWS IAM integration is planned.
- `masking_control.dead_letters` has no index on `job_id`. For jobs with high substitution volumes, add one manually: `db.dead_letters.createIndex({ job_id: 1 })`.
