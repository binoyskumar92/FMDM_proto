import argparse
import json
import logging
import math
import os
import shutil
import subprocess
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ProcessPoolExecutor

import bson
import pymongo
from pymongo import UpdateOne, MongoClient
from pymongo.errors import BulkWriteError
from bson.objectid import ObjectId

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(processName)s - %(message)s",
)


# ==========================================
# MODULE 1: PLUGGABLE TRANSFORM ENGINE
# ==========================================
def transform_document_placeholder(doc: dict) -> dict:
    """
    FUTURE MASKING ENGINE INJECTION POINT.
    Currently a no-op to establish baseline benchmark.
    """
    return doc


# ==========================================
# MODULE 2: BOOKKEEPING & LEASING
# ==========================================
def initialize_bookkeeping(db_uri: str, job_config: dict):
    """Creates the control DB and mandatory indexes for atomic leasing."""
    client = MongoClient(db_uri)
    db = client["masking_control"]

    db.jobs.create_index("job_id", unique=True)
    db.chunks.create_index([("job_id", 1), ("chunk_sequence", 1)], unique=True)
    db.chunks.create_index([("job_id", 1), ("status", 1)])
    db.chunks.create_index([("job_id", 1), ("status", 1), ("lease_expires_at", 1)])

    # Initialize or resume Job record
    job = db.jobs.find_one({"job_id": job_config["job_id"]})
    if not job:
        db.jobs.insert_one(
            {
                "job_id": job_config["job_id"],
                "mode": job_config["mode"],
                "status": "CREATED",
                "created_at": datetime.now(timezone.utc),
                "config_snapshot": job_config,
            }
        )
    else:
        logging.info(f"Resuming existing job: {job_config['job_id']}")
        # Clear expired leases on resume
        db.chunks.update_many(
            {
                "job_id": job_config["job_id"],
                "status": {"$in": ["STREAMING", "DUMPING", "LOADING"]},
            },
            {
                "$set": {
                    "status": (
                        "READY"
                        if job_config["mode"] == "DIRECT_STREAM"
                        else "READY_TO_DUMP"
                    ),
                    "lease_owner": None,
                    "lease_expires_at": None,
                }
            },
        )


def lease_next_chunk(
    chunks_coll,
    job_id: str,
    ready_status: str,
    active_status: str,
    worker_id: str,
    max_attempts: int = 3,
    lease_hours: float = 1,
):
    """Atomically finds and leases the next available chunk."""
    now = datetime.now(timezone.utc)
    lease_expiration = now + timedelta(hours=lease_hours)

    # Find a chunk that is ready, OR a chunk whose lease has expired, AND is under max attempts
    query = {
        "job_id": job_id,
        "$or": [
            {"status": ready_status},
            {"status": active_status, "lease_expires_at": {"$lt": now}},
            {
                "status": f"FAILED_{active_status.split('ING')[0]}"
            },  # Retry failed states
        ],
        "attempts": {"$lt": max_attempts},
    }

    update = {
        "$set": {
            "status": active_status,
            "lease_owner": worker_id,
            "lease_expires_at": lease_expiration,
            "started_at": now,
        },
        "$inc": {"attempts": 1},
    }

    return chunks_coll.find_one_and_update(
        query,
        update,
        sort=[("chunk_sequence", 1)],
        return_document=pymongo.ReturnDocument.AFTER,
    )


# ==========================================
# MODULE 3: DENSITY-AWARE CHUNK PLANNER
# ==========================================
def _count_id_range(coll, lower_bound_oid, upper_bound_oid, cap=None):
    """
    Count documents in an _id range using the _id index.

    If cap is provided, this intentionally returns at most cap + 1. This is
    faster for planner decisions because most calls only need to know whether
    a range is over the configured max_docs_per_chunk threshold.
    """
    query = {"_id": {"$gte": lower_bound_oid, "$lt": upper_bound_oid}}

    if cap is None:
        return coll.count_documents(query, hint="_id_")

    # count_documents supports limit. This avoids scanning a huge hot range
    # when all we need is "over max" vs "safe".
    return coll.count_documents(query, hint="_id_", limit=cap + 1)


def _objectid_to_int(oid: ObjectId) -> int:
    return int.from_bytes(oid.binary, "big")


def _int_to_objectid(value: int) -> ObjectId:
    return ObjectId(value.to_bytes(12, "big"))


def _next_real_doc_id(coll, lower_bound_oid, inclusive=True):
    """Return the next real document _id at or after a boundary."""
    operator = "$gte" if inclusive else "$gt"
    docs = list(
        coll.find({"_id": {operator: lower_bound_oid}}, {"_id": 1})
        .sort("_id", 1)
        .limit(1)
        .hint("_id_")
    )
    return docs[0]["_id"] if docs else None


def _binary_search_upper_bound_by_objectid(
    coll,
    lower_bound_oid,
    high_upper_oid,
    max_docs_per_chunk,
    max_steps=96,
):
    """
    Split within ObjectId byte space when a one-second timestamp window is
    still too dense.

    This matters for synthetic load tests where many documents can share the
    same ObjectId generation_time second. Time-based splitting alone cannot
    safely split those hot spots.
    """
    low_int = _objectid_to_int(lower_bound_oid) + 1
    high_int = _objectid_to_int(high_upper_oid)

    if low_int >= high_int:
        doc_count = _count_id_range(
            coll, lower_bound_oid, high_upper_oid, cap=max_docs_per_chunk
        )
        return high_upper_oid, doc_count

    best_upper_oid = None
    best_count = 0
    steps = 0

    while low_int <= high_int and steps < max_steps:
        steps += 1
        mid_int = (low_int + high_int) // 2

        # The upper bound is exclusive, so it must be strictly above lower.
        if mid_int <= _objectid_to_int(lower_bound_oid):
            low_int = mid_int + 1
            continue

        mid_oid = _int_to_objectid(mid_int)
        doc_count = _count_id_range(
            coll, lower_bound_oid, mid_oid, cap=max_docs_per_chunk
        )

        if doc_count <= max_docs_per_chunk:
            best_upper_oid = mid_oid
            best_count = doc_count
            low_int = mid_int + 1
        else:
            high_int = mid_int - 1

    if best_upper_oid is not None and best_upper_oid > lower_bound_oid:
        return best_upper_oid, best_count

    # Last-resort progress guard. This should only happen with invalid config,
    # for example max_docs_per_chunk <= 0. It prevents an infinite planning loop.
    fallback_upper_oid = _int_to_objectid(_objectid_to_int(lower_bound_oid) + 1)
    fallback_count = _count_id_range(
        coll, lower_bound_oid, fallback_upper_oid, cap=max_docs_per_chunk
    )
    return fallback_upper_oid, fallback_count


def get_safe_upper_bound(
    coll,
    lower_bound_oid,
    naive_upper_oid,
    max_docs_per_chunk,
    min_window_seconds=1,
):
    """
    Find the largest safe upper bound that does not exceed max_docs_per_chunk.

    Improvements over the original recursive halving:
      - Uses the _id index hint for all planner counts.
      - Uses capped counts so hot ranges do not fully scan during planning.
      - Uses binary search instead of recursive midpoint-only shrink.
      - Falls back to raw ObjectId byte splitting for one-second hot spots.
    """
    doc_count = _count_id_range(
        coll, lower_bound_oid, naive_upper_oid, cap=max_docs_per_chunk
    )

    if doc_count <= max_docs_per_chunk:
        return naive_upper_oid, doc_count

    lower_time = lower_bound_oid.generation_time
    upper_time = naive_upper_oid.generation_time
    time_diff_seconds = (upper_time - lower_time).total_seconds()

    # If timestamp-level splitting cannot reduce this further, split the raw
    # ObjectId byte range so a hot second can still be chunked safely.
    if time_diff_seconds <= min_window_seconds:
        return _binary_search_upper_bound_by_objectid(
            coll, lower_bound_oid, naive_upper_oid, max_docs_per_chunk
        )

    low_time = lower_time
    high_time = upper_time
    best_upper_oid = None
    best_count = 0

    # 64 steps is far more than enough for second-level precision across years.
    for _ in range(64):
        remaining_seconds = (high_time - low_time).total_seconds()
        if remaining_seconds <= min_window_seconds:
            break

        midpoint_time = low_time + timedelta(seconds=remaining_seconds / 2)
        midpoint_oid = ObjectId.from_datetime(midpoint_time)

        # ObjectId.from_datetime truncates to timestamp seconds and zeroes the
        # suffix. Within the same second this can fail to advance past lower.
        if midpoint_oid <= lower_bound_oid:
            break

        doc_count = _count_id_range(
            coll, lower_bound_oid, midpoint_oid, cap=max_docs_per_chunk
        )

        if doc_count <= max_docs_per_chunk:
            best_upper_oid = midpoint_oid
            best_count = doc_count
            low_time = midpoint_time
        else:
            high_time = midpoint_time

    if best_upper_oid is not None and best_upper_oid > lower_bound_oid:
        return best_upper_oid, best_count

    # If time-level binary search could not produce a safe boundary, split the
    # raw ObjectId interval. This handles dense same-second distributions.
    return _binary_search_upper_bound_by_objectid(
        coll, lower_bound_oid, naive_upper_oid, max_docs_per_chunk
    )


def get_optimal_upper_bound(
    coll,
    lower_bound_oid,
    initial_window_hours,
    max_docs_per_chunk,
    max_expansion_hours=24,
):
    """
    Bi-directional planner:
    Expands time window if under 50% capacity, shrinks if over 100% capacity.
    """
    min_docs_per_chunk = max(1, int(max_docs_per_chunk * 0.5))

    lower_time = lower_bound_oid.generation_time
    current_window = timedelta(hours=initial_window_hours)
    max_window = timedelta(hours=max_expansion_hours)
    naive_upper_oid = ObjectId.from_datetime(lower_time + current_window)

    doc_count = _count_id_range(
        coll, lower_bound_oid, naive_upper_oid, cap=max_docs_per_chunk
    )

    # If exactly 0, return immediately so the fast-forward logic in the main
    # loop can skip the gap.
    if doc_count == 0:
        return naive_upper_oid, 0

    # 1. EXPANSION PHASE (Too few docs, "The Trickle")
    # Keep doubling the window until we hit 50% capacity or the safety cap.
    while doc_count < min_docs_per_chunk and current_window < max_window:
        next_window = min(current_window * 2, max_window)

        # No progress guard for odd config values.
        if next_window <= current_window:
            break

        current_window = next_window
        naive_upper_oid = ObjectId.from_datetime(lower_time + current_window)
        doc_count = _count_id_range(
            coll, lower_bound_oid, naive_upper_oid, cap=max_docs_per_chunk
        )

        if doc_count == 0:
            return naive_upper_oid, 0

    # 2. SHRINK PHASE (Too many docs, "The Spike")
    if doc_count > max_docs_per_chunk:
        return get_safe_upper_bound(
            coll, lower_bound_oid, naive_upper_oid, max_docs_per_chunk
        )

    return naive_upper_oid, doc_count


def plan_chunks(job_config):
    """Generates sequential, density-aware chunk boundaries with timer."""
    bk_client = MongoClient(job_config['bookkeeping_uri'])
    db = bk_client["masking_control"]

    # If chunks already exist, skip planning (Resume mode)
    if db.chunks.count_documents({"job_id": job_config["job_id"]}) > 0:
        logging.info("Chunks already exist. Skipping planning phase.")
        return

    db.jobs.update_one({"job_id": job_config["job_id"]}, {"$set": {"status": "PLANNING"}})

    src_client = MongoClient(job_config['source_uri'])
    coll = src_client[job_config['source_db']][job_config['source_collection']]

    min_doc = list(coll.find({}, {"_id": 1}).sort("_id", 1).limit(1).hint("_id_"))
    max_doc = list(coll.find({}, {"_id": 1}).sort("_id", -1).limit(1).hint("_id_"))

    if not min_doc or not max_doc:
        logging.info("Source collection is empty.")
        db.jobs.update_one({"job_id": job_config["job_id"]}, {"$set": {"status": "COMPLETED"}})
        return

    global_min_id = min_doc[0]["_id"]
    global_max_id = max_doc[0]["_id"]

    current_lower_bound = global_min_id
    seq = 1
    ready_status = "READY" if job_config["mode"] == "DIRECT_STREAM" else "READY_TO_DUMP"
    chunks_to_insert = []

    logging.info("Starting bi-directional density-aware chunk planning...")
    planning_start_time = time.time() # START CLOCK

    while current_lower_bound < global_max_id:

        # Calculate optimal boundary using expand/shrink logic
        safe_upper_bound, estimated_count = get_optimal_upper_bound(
            coll,
            current_lower_bound,
            job_config["chunk_window_hours"],
            job_config["max_docs_per_chunk"],
        )

        # Fast-forward over empty gaps (Gap Compression)
        if estimated_count == 0:
            next_real_doc_id = _next_real_doc_id(coll, safe_upper_bound, inclusive=True)
            if not next_real_doc_id:
                break
            current_lower_bound = next_real_doc_id
            continue

        # Defensive progress guard. Without this, a pathological boundary can
        # loop forever. This keeps the half-open range invariant intact:
        # [current_lower_bound, safe_upper_bound)
        if safe_upper_bound <= current_lower_bound:
            next_real_doc_id = _next_real_doc_id(coll, current_lower_bound, inclusive=False)
            if not next_real_doc_id:
                break
            safe_upper_bound = next_real_doc_id
            estimated_count = _count_id_range(
                coll,
                current_lower_bound,
                safe_upper_bound,
                cap=job_config["max_docs_per_chunk"],
            )

        chunks_to_insert.append({
            "chunk_id": f"{job_config['job_id']}_{seq}",
            "job_id": job_config["job_id"],
            "chunk_sequence": seq,
            "lower_bound": current_lower_bound,
            "upper_bound": safe_upper_bound,
            # Keep old field for backward compatibility with existing tests and
            # metrics, even though it is a document count, not bytes.
            "bytes_read_estimate": estimated_count,
            # New correctly named alias for future code.
            "docs_read_estimate": estimated_count,
            "status": ready_status,
            "attempts": 0
        })

        current_lower_bound = safe_upper_bound
        seq += 1

        # Batch insert to bookkeeping to save memory
        if len(chunks_to_insert) >= 1000:
            db.chunks.insert_many(chunks_to_insert)
            chunks_to_insert = []

    # STOP CLOCK
    planning_duration = time.time() - planning_start_time

    if chunks_to_insert:
        db.chunks.insert_many(chunks_to_insert)

    db.jobs.update_one(
        {"job_id": job_config["job_id"]},
        {"$set": {
            "status": "RUNNING",
            "total_chunks": seq - 1,
            "planning_duration_seconds": round(planning_duration, 2)
        }}
    )
    logging.info(f"Planning complete in {round(planning_duration, 2)} seconds. Created {seq - 1} chunks.")




# ==========================================
# DESTINATION WRITE STRATEGIES
# ==========================================
DEST_WRITE_MODE_UPSERT = "UPSERT"
DEST_WRITE_MODE_INSERT_THEN_UPSERT_ON_DUP = "INSERT_THEN_UPSERT_ON_DUP"
SUPPORTED_DEST_WRITE_MODES = {
    DEST_WRITE_MODE_UPSERT,
    DEST_WRITE_MODE_INSERT_THEN_UPSERT_ON_DUP,
}


def _empty_write_stats() -> dict:
    return {
        "docs_seen": 0,
        "docs_inserted": 0,
        "docs_matched": 0,
        "docs_modified": 0,
        "docs_upserted": 0,
        "docs_upserted_after_duplicate": 0,
        "duplicate_key_errors": 0,
        "batches_written": 0,
    }


def _merge_write_stats(total: dict, increment: dict) -> dict:
    for key, value in increment.items():
        total[key] = total.get(key, 0) + value
    return total


def _get_dest_write_mode(job_config: dict) -> str:
    """
    Keep this intentionally simple. There are only two supported write modes:

      UPSERT
        Original behavior. Every write is UpdateOne(..., upsert=True).
        Best resumability and simplest operational behavior.

      INSERT_THEN_UPSERT_ON_DUP
        Fast path is insert_many(..., ordered=False).
        If a retry hits duplicate _id errors, only those duplicate docs are
        upserted. This keeps resumability while making clean chunks use inserts.
    """
    mode = job_config.get("dest_write_mode", DEST_WRITE_MODE_UPSERT).upper()

    if mode not in SUPPORTED_DEST_WRITE_MODES:
        raise ValueError(
            f"Unsupported dest_write_mode: {mode!r}. "
            f"Supported values: {sorted(SUPPORTED_DEST_WRITE_MODES)}"
        )

    return mode


def _is_duplicate_key_error(error_doc: dict) -> bool:
    return error_doc.get("code") == 11000


def write_dest_batch(dest_coll, docs: list, job_config: dict) -> dict:
    """
    Write a batch of transformed destination documents.

    Mode 1, UPSERT:
      Uses bulk_write with UpdateOne(..., upsert=True) for every document.
      This is the safest and original behavior.

    Mode 2, INSERT_THEN_UPSERT_ON_DUP:
      Uses insert_many first. If duplicate key errors happen, only those
      duplicate docs are retried using upsert. Non-duplicate write errors fail
      the chunk.

    The second mode is still resumable:
      - If a chunk partially wrote before a crash, retrying the chunk inserts
        missing docs and upserts duplicate docs.
      - If a chunk fully wrote but failed before status update, retrying the
        chunk upserts the duplicate docs and then marks the chunk completed.
    """
    if not docs:
        return _empty_write_stats()

    mode = _get_dest_write_mode(job_config)

    if mode == DEST_WRITE_MODE_UPSERT:
        ops = [
            UpdateOne(
                {"_id": doc["_id"]},
                {"$set": doc},
                upsert=True,
            )
            for doc in docs
        ]

        result = dest_coll.bulk_write(ops, ordered=False)
        return {
            "docs_seen": len(docs),
            "docs_inserted": 0,
            "docs_matched": result.matched_count,
            "docs_modified": result.modified_count,
            "docs_upserted": result.upserted_count,
            "docs_upserted_after_duplicate": 0,
            "duplicate_key_errors": 0,
            "batches_written": 1,
        }

    # INSERT_THEN_UPSERT_ON_DUP
    try:
        result = dest_coll.insert_many(docs, ordered=False)
        return {
            "docs_seen": len(docs),
            "docs_inserted": len(result.inserted_ids),
            "docs_matched": 0,
            "docs_modified": 0,
            "docs_upserted": 0,
            "docs_upserted_after_duplicate": 0,
            "duplicate_key_errors": 0,
            "batches_written": 1,
        }

    except BulkWriteError as exc:
        details = exc.details or {}
        write_errors = details.get("writeErrors", [])

        non_duplicate_errors = [
            err for err in write_errors
            if not _is_duplicate_key_error(err)
        ]
        if non_duplicate_errors:
            raise

        duplicate_indexes = sorted(
            {
                err["index"]
                for err in write_errors
                if _is_duplicate_key_error(err) and "index" in err
            }
        )

        duplicate_docs = [
            docs[i]
            for i in duplicate_indexes
            if 0 <= i < len(docs)
        ]

        stats = {
            "docs_seen": len(docs),
            "docs_inserted": details.get("nInserted", 0),
            "docs_matched": 0,
            "docs_modified": 0,
            "docs_upserted": 0,
            "docs_upserted_after_duplicate": 0,
            "duplicate_key_errors": len(duplicate_docs),
            "batches_written": 1,
        }

        if duplicate_docs:
            ops = [
                UpdateOne(
                    {"_id": doc["_id"]},
                    {"$set": doc},
                    upsert=True,
                )
                for doc in duplicate_docs
            ]

            fallback_result = dest_coll.bulk_write(ops, ordered=False)
            stats["docs_matched"] += fallback_result.matched_count
            stats["docs_modified"] += fallback_result.modified_count
            stats["docs_upserted"] += fallback_result.upserted_count
            stats["docs_upserted_after_duplicate"] += len(duplicate_docs)
            stats["batches_written"] += 1

        return stats


# ==========================================
# MODULE 4: WORKER IMPLEMENTATIONS
# ==========================================
def direct_stream_worker(worker_id: str, job_config: dict):
    src_client = MongoClient(job_config["source_uri"])
    dest_client = MongoClient(job_config["dest_uri"])
    bk_client = MongoClient(job_config["bookkeeping_uri"])

    src_coll = src_client[job_config["source_db"]][job_config["source_collection"]]
    dest_coll = dest_client[job_config["dest_db"]][job_config["dest_collection"]]
    chunks_coll = bk_client["masking_control"]["chunks"]

    while True:
        chunk = lease_next_chunk(
            chunks_coll,
            job_config["job_id"],
            "READY",
            "STREAMING",
            worker_id,
            job_config["max_retries"],
            job_config.get("lease_hours", 1),
        )
        if not chunk:
            time.sleep(5)
            # Break condition managed by orchestrator in real app, simplistic exit here
            if (
                chunks_coll.count_documents(
                    {
                        "job_id": job_config["job_id"],
                        "status": {"$in": ["READY", "STREAMING"]},
                    }
                )
                == 0
            ):
                break
            continue

        try:
            start_time = time.time()
            query = {"_id": {"$gte": chunk["lower_bound"], "$lt": chunk["upper_bound"]}}
            cursor = (
                src_coll.find(query)
                .hint("_id_")
                .batch_size(job_config["src_batch_size"])
            )

            batch = []
            docs_read = 0
            write_stats = _empty_write_stats()

            for doc in cursor:
                docs_read += 1
                transformed_doc = transform_document_placeholder(doc)
                batch.append(transformed_doc)

                if len(batch) >= job_config["dest_batch_size"]:
                    _merge_write_stats(
                        write_stats,
                        write_dest_batch(dest_coll, batch, job_config),
                    )
                    batch = []

            if batch:
                _merge_write_stats(
                    write_stats,
                    write_dest_batch(dest_coll, batch, job_config),
                )

            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {
                        "status": "LOADED",
                        "docs_written": docs_read,
                        "stream_duration": time.time() - start_time,
                        "dest_write_mode": _get_dest_write_mode(job_config),
                        "docs_inserted": write_stats.get("docs_inserted", 0),
                        "docs_matched": write_stats.get("docs_matched", 0),
                        "docs_modified": write_stats.get("docs_modified", 0),
                        "docs_upserted": write_stats.get("docs_upserted", 0),
                        "docs_upserted_after_duplicate": write_stats.get("docs_upserted_after_duplicate", 0),
                        "duplicate_key_errors": write_stats.get("duplicate_key_errors", 0),
                        "batches_written": write_stats.get("batches_written", 0),
                    }
                },
            )

        except Exception as e:
            logging.error(
                f"Worker {worker_id} failed chunk {chunk['chunk_sequence']}: {e}"
            )
            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"status": "FAILED_STREAM", "last_error_sanitized": str(e)}},
            )


def mongodump_worker(worker_id: str, job_config: dict):
    bk_client = MongoClient(job_config["bookkeeping_uri"])
    chunks_coll = bk_client["masking_control"]["chunks"]

    while True:
        # Enforce Backpressure
        staged_chunks = chunks_coll.count_documents(
            {"job_id": job_config["job_id"], "status": "DUMPED"}
        )
        if staged_chunks >= job_config["max_backlog_chunks"]:
            logging.debug(f"Dump {worker_id} pausing due to backpressure...")
            time.sleep(5)
            continue

        chunk = lease_next_chunk(
            chunks_coll,
            job_config["job_id"],
            "READY_TO_DUMP",
            "DUMPING",
            worker_id,
            job_config["max_retries"],
            job_config.get("lease_hours", 1),
        )
        if not chunk:
            time.sleep(5)
            if (
                chunks_coll.count_documents(
                    {
                        "job_id": job_config["job_id"],
                        "status": {"$in": ["READY_TO_DUMP", "DUMPING"]},
                    }
                )
                == 0
            ):
                break
            continue

        try:
            start_time = time.time()
            chunk_dir = os.path.join(
                job_config["staging_root"], job_config["job_id"], chunk["chunk_id"]
            )

            # Clean partial leftovers if retrying
            if os.path.exists(chunk_dir):
                shutil.rmtree(chunk_dir)
            os.makedirs(chunk_dir)

            query_str = f'{{"_id": {{"$gte": {{"$oid": "{str(chunk["lower_bound"])}"}}, "$lt": {{"$oid": "{str(chunk["upper_bound"])}"}} }} }}'

            cmd = [
                "mongodump",
                "--uri",
                job_config["source_uri"],
                "--db",
                job_config["source_db"],
                "--collection",
                job_config["source_collection"],
                "--query",
                query_str,
                "--out",
                chunk_dir,
            ]

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Exit {result.returncode}: {result.stderr[:200]}")

            # Locate BSON
            dumped_db_dir = os.path.join(chunk_dir, job_config["source_db"])
            bson_file = os.path.join(
                dumped_db_dir, f"{job_config['source_collection']}.bson"
            )

            dump_bytes = os.path.getsize(bson_file) if os.path.exists(bson_file) else 0

            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {
                        "status": "DUMPED",
                        "dump_folder": chunk_dir,
                        "bson_file_path": bson_file,
                        "dump_bytes": dump_bytes,
                        "dump_duration": time.time() - start_time,
                    }
                },
            )

        except Exception as e:
            logging.error(
                f"Dump {worker_id} failed chunk {chunk['chunk_sequence']}: {e}"
            )
            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"status": "FAILED_DUMP", "last_error_sanitized": str(e)}},
            )


def bson_load_worker(worker_id: str, job_config: dict):
    dest_client = MongoClient(job_config["dest_uri"])
    dest_coll = dest_client[job_config["dest_db"]][job_config["dest_collection"]]
    bk_client = MongoClient(job_config["bookkeeping_uri"])
    chunks_coll = bk_client["masking_control"]["chunks"]

    while True:
        chunk = lease_next_chunk(
            chunks_coll,
            job_config["job_id"],
            "DUMPED",
            "LOADING",
            worker_id,
            job_config["max_retries"],
            job_config.get("lease_hours", 1),
        )
        if not chunk:
            time.sleep(5)
            # Exit if no chunks remain anywhere in the pipeline
            active_states = ["READY_TO_DUMP", "DUMPING", "DUMPED", "LOADING"]
            if (
                chunks_coll.count_documents(
                    {"job_id": job_config["job_id"], "status": {"$in": active_states}}
                )
                == 0
            ):
                break
            continue

        try:
            start_time = time.time()
            bson_file = chunk.get("bson_file_path")
            batch = []
            docs_written = 0

            write_stats = _empty_write_stats()

            if bson_file and os.path.exists(bson_file):
                # Stream BSON file iteratively (Crucial for memory safety)
                with open(bson_file, "rb") as f:
                    for doc in bson.decode_file_iter(f):
                        docs_written += 1
                        transformed_doc = transform_document_placeholder(doc)
                        batch.append(transformed_doc)

                        if len(batch) >= job_config["dest_batch_size"]:
                            _merge_write_stats(
                                write_stats,
                                write_dest_batch(dest_coll, batch, job_config),
                            )
                            batch = []

                if batch:
                    _merge_write_stats(
                        write_stats,
                        write_dest_batch(dest_coll, batch, job_config),
                    )

            # Transition to loaded, then trigger cleanup immediately
            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {
                        "status": "LOADED",
                        "docs_written": docs_written,
                        "dest_write_mode": _get_dest_write_mode(job_config),
                        "docs_inserted": write_stats.get("docs_inserted", 0),
                        "docs_matched": write_stats.get("docs_matched", 0),
                        "docs_modified": write_stats.get("docs_modified", 0),
                        "docs_upserted": write_stats.get("docs_upserted", 0),
                        "docs_upserted_after_duplicate": write_stats.get("docs_upserted_after_duplicate", 0),
                        "duplicate_key_errors": write_stats.get("duplicate_key_errors", 0),
                        "batches_written": write_stats.get("batches_written", 0),
                    }
                },
            )

            if chunk.get("dump_folder") and os.path.exists(chunk["dump_folder"]):
                shutil.rmtree(chunk["dump_folder"])

            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {
                        "status": "RAW_DELETED",
                        "load_duration": time.time() - start_time,
                    }
                },
            )

        except Exception as e:
            logging.error(
                f"Load {worker_id} failed chunk {chunk['chunk_sequence']}: {e}"
            )
            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"status": "FAILED_LOAD", "last_error_sanitized": str(e)}},
            )


# ==========================================
# MODULE 5: ORCHESTRATOR
# ==========================================
def run_benchmark(config_file_path: str):
    with open(config_file_path, "r") as f:
        job_config = json.load(f)

    logging.info(
        f"Starting ETL Benchmark Job: {job_config['job_id']} in mode: {job_config['mode']}"
    )

    initialize_bookkeeping(job_config["bookkeeping_uri"], job_config)
    plan_chunks(job_config)

    overall_start = time.time()

    if job_config["mode"] == "DIRECT_STREAM":
        workers = job_config["stream_workers"]
        with ProcessPoolExecutor(max_workers=workers) as executor:
            for i in range(workers):
                executor.submit(direct_stream_worker, f"stream_{i}", job_config)

    elif job_config["mode"] == "MONGODUMP_STAGE":
        dump_workers = job_config["dump_workers"]
        load_workers = job_config["load_workers"]
        with ProcessPoolExecutor(max_workers=(dump_workers + load_workers)) as executor:
            for i in range(dump_workers):
                executor.submit(mongodump_worker, f"dump_{i}", job_config)
            for i in range(load_workers):
                executor.submit(bson_load_worker, f"load_{i}", job_config)

    overall_duration = time.time() - overall_start

    # ----------------------------------------
    # FINAL METRICS COLLECTION
    # ----------------------------------------
    bk_client = MongoClient(job_config["bookkeeping_uri"])
    db = bk_client["masking_control"]

    target_status = "LOADED" if job_config["mode"] == "DIRECT_STREAM" else "RAW_DELETED"

    pipeline = [
        {"$match": {"job_id": job_config["job_id"], "status": target_status}},
        {
            "$group": {
                "_id": None,
                "total_docs": {"$sum": "$docs_written"},
                "total_dump_bytes": {"$sum": {"$ifNull": ["$dump_bytes", 0]}},
                "completed_chunks": {"$sum": 1},
                "total_docs_inserted": {"$sum": {"$ifNull": ["$docs_inserted", 0]}},
                "total_docs_matched": {"$sum": {"$ifNull": ["$docs_matched", 0]}},
                "total_docs_modified": {"$sum": {"$ifNull": ["$docs_modified", 0]}},
                "total_docs_upserted": {"$sum": {"$ifNull": ["$docs_upserted", 0]}},
                "total_docs_upserted_after_duplicate": {"$sum": {"$ifNull": ["$docs_upserted_after_duplicate", 0]}},
                "total_duplicate_key_errors": {"$sum": {"$ifNull": ["$duplicate_key_errors", 0]}},
                "total_batches_written": {"$sum": {"$ifNull": ["$batches_written", 0]}},
            }
        },
    ]

    stats = list(db.chunks.aggregate(pipeline))
    metrics = (
        stats[0]
        if stats
        else {
            "total_docs": 0,
            "total_dump_bytes": 0,
            "completed_chunks": 0,
            "total_docs_inserted": 0,
            "total_docs_matched": 0,
            "total_docs_modified": 0,
            "total_docs_upserted": 0,
            "total_docs_upserted_after_duplicate": 0,
            "total_duplicate_key_errors": 0,
            "total_batches_written": 0,
        }
    )

    docs_per_sec = (
        metrics["total_docs"] / overall_duration if overall_duration > 0 else 0
    )
    mb_per_sec = (
        (metrics["total_dump_bytes"] / (1024 * 1024)) / overall_duration
        if overall_duration > 0
        else 0
    )

    summary = {
        "status": "COMPLETED",
        "total_runtime_seconds": round(overall_duration, 2),
        "total_docs_written": metrics["total_docs"],
        "total_dump_bytes": metrics["total_dump_bytes"],
        "completed_chunks": metrics["completed_chunks"],
        "total_docs_inserted": metrics.get("total_docs_inserted", 0),
        "total_docs_matched": metrics.get("total_docs_matched", 0),
        "total_docs_modified": metrics.get("total_docs_modified", 0),
        "total_docs_upserted": metrics.get("total_docs_upserted", 0),
        "total_docs_upserted_after_duplicate": metrics.get("total_docs_upserted_after_duplicate", 0),
        "total_duplicate_key_errors": metrics.get("total_duplicate_key_errors", 0),
        "total_batches_written": metrics.get("total_batches_written", 0),
        "dest_write_mode": _get_dest_write_mode(job_config),
        "effective_docs_per_second": round(docs_per_sec, 2),
        "effective_mb_per_second": round(mb_per_sec, 2),
        "finished_at": datetime.now(timezone.utc),
    }

    db.jobs.update_one({"job_id": job_config["job_id"]}, {"$set": summary})

    logging.info("=========================================")
    logging.info("BENCHMARK SUMMARY")
    logging.info("=========================================")
    for k, v in summary.items():
        logging.info(f"{k}: {v}")
    logging.info("=========================================")


# ==========================================
# MAIN ENTRYPOINT
# ==========================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MongoDB ETL Benchmark Prototype")
    parser.add_argument("--config", required=True, help="Path to JSON config file")
    args = parser.parse_args()

    run_benchmark(args.config)
