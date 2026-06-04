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
):
    """Atomically finds and leases the next available chunk."""
    now = datetime.now(timezone.utc)
    lease_expiration = now + timedelta(hours=1)

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
def get_safe_upper_bound(
    coll, lower_bound_oid, naive_upper_oid, max_docs_per_chunk, min_window_seconds=1
):
    """Recursively shrinks time window using index counts to prevent hot chunk skew."""
    doc_count = coll.count_documents(
        {"_id": {"$gte": lower_bound_oid, "$lt": naive_upper_oid}}
    )

    if doc_count <= max_docs_per_chunk:
        return naive_upper_oid, doc_count

    lower_time = lower_bound_oid.generation_time
    upper_time = naive_upper_oid.generation_time
    time_diff_seconds = (upper_time - lower_time).total_seconds()

    if time_diff_seconds <= min_window_seconds:
        return naive_upper_oid, doc_count

    halfway_time = lower_time + timedelta(seconds=time_diff_seconds / 2)
    new_naive_upper_oid = ObjectId.from_datetime(halfway_time)

    return get_safe_upper_bound(
        coll,
        lower_bound_oid,
        new_naive_upper_oid,
        max_docs_per_chunk,
        min_window_seconds,
    )


def plan_chunks(job_config):
    """Generates sequential, density-aware chunk boundaries."""
    bk_client = MongoClient(job_config["bookkeeping_uri"])
    db = bk_client["masking_control"]

    # If chunks already exist, skip planning (Resume mode)
    if db.chunks.count_documents({"job_id": job_config["job_id"]}) > 0:
        logging.info("Chunks already exist. Skipping planning phase.")
        return

    db.jobs.update_one(
        {"job_id": job_config["job_id"]}, {"$set": {"status": "PLANNING"}}
    )

    src_client = MongoClient(job_config["source_uri"])
    coll = src_client[job_config["source_db"]][job_config["source_collection"]]

    min_doc = list(coll.find({}, {"_id": 1}).sort("_id", 1).limit(1).hint("_id_"))
    max_doc = list(coll.find({}, {"_id": 1}).sort("_id", -1).limit(1).hint("_id_"))

    if not min_doc or not max_doc:
        logging.info("Source collection is empty.")
        db.jobs.update_one(
            {"job_id": job_config["job_id"]}, {"$set": {"status": "COMPLETED"}}
        )
        return

    global_min_id = min_doc[0]["_id"]
    global_max_id = max_doc[0]["_id"]

    current_lower_bound = global_min_id
    seq = 1
    ready_status = "READY" if job_config["mode"] == "DIRECT_STREAM" else "READY_TO_DUMP"
    chunks_to_insert = []

    logging.info("Starting density-aware chunk planning...")
    planning_start_time = time.time()
    while current_lower_bound < global_max_id:
        naive_next_time = current_lower_bound.generation_time + timedelta(
            hours=job_config["chunk_window_hours"]
        )
        naive_upper_bound = ObjectId.from_datetime(naive_next_time)

        safe_upper_bound, estimated_count = get_safe_upper_bound(
            coll,
            current_lower_bound,
            naive_upper_bound,
            job_config["max_docs_per_chunk"],
        )

        # Fast-forward over empty gaps (Gap Compression)
        if estimated_count == 0:
            next_real_doc = list(
                coll.find({"_id": {"$gte": safe_upper_bound}}, {"_id": 1})
                .sort("_id", 1)
                .limit(1)
                .hint("_id_")
            )
            if not next_real_doc:
                break
            current_lower_bound = next_real_doc[0]["_id"]
            continue

        chunks_to_insert.append(
            {
                "chunk_id": f"{job_config['job_id']}_{seq}",
                "job_id": job_config["job_id"],
                "chunk_sequence": seq,
                "lower_bound": current_lower_bound,
                "upper_bound": safe_upper_bound,
                "bytes_read_estimate": estimated_count,
                "status": ready_status,
                "attempts": 0,
            }
        )

        current_lower_bound = safe_upper_bound
        seq += 1

        # Batch insert to bookkeeping to save memory
        if len(chunks_to_insert) >= 1000:
            db.chunks.insert_many(chunks_to_insert)
            chunks_to_insert = []

    planning_duration = time.time() - planning_start_time
    if chunks_to_insert:
        db.chunks.insert_many(chunks_to_insert)

    db.jobs.update_one(
        {"job_id": job_config["job_id"]},
        {"$set": {"status": "RUNNING", "total_chunks": seq - 1, "planning_duration_seconds": round(planning_duration, 2)}},
    )
    logging.info(f"Planning complete in {round(planning_duration, 2)} seconds. Created {seq - 1} chunks.")


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

            for doc in cursor:
                docs_read += 1
                transformed_doc = transform_document_placeholder(doc)
                batch.append(
                    UpdateOne(
                        {"_id": transformed_doc["_id"]},
                        {"$set": transformed_doc},
                        upsert=True,
                    )
                )

                if len(batch) >= job_config["dest_batch_size"]:
                    dest_coll.bulk_write(batch, ordered=False)
                    batch = []

            if batch:
                dest_coll.bulk_write(batch, ordered=False)

            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {
                    "$set": {
                        "status": "LOADED",
                        "docs_written": docs_read,
                        "stream_duration": time.time() - start_time,
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

            if bson_file and os.path.exists(bson_file):
                # Stream BSON file iteratively (Crucial for memory safety)
                with open(bson_file, "rb") as f:
                    for doc in bson.decode_file_iter(f):
                        docs_written += 1
                        transformed_doc = transform_document_placeholder(doc)
                        batch.append(
                            UpdateOne(
                                {"_id": transformed_doc["_id"]},
                                {"$set": transformed_doc},
                                upsert=True,
                            )
                        )

                        if len(batch) >= job_config["dest_batch_size"]:
                            dest_coll.bulk_write(batch, ordered=False)
                            batch = []

                if batch:
                    dest_coll.bulk_write(batch, ordered=False)

            # Transition to loaded, then trigger cleanup immediately
            chunks_coll.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"status": "LOADED", "docs_written": docs_written}},
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
            }
        },
    ]

    stats = list(db.chunks.aggregate(pipeline))
    metrics = (
        stats[0]
        if stats
        else {"total_docs": 0, "total_dump_bytes": 0, "completed_chunks": 0}
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
