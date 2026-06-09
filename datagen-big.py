#!/usr/bin/env python3
"""
MongoDB load-test data generator.

Purpose:
  - Read an existing organic source collection.
  - Duplicate documents until a target size or multiplier is reached.
  - Generate new ObjectIds whose timestamp portion is spread across a historical range.
  - Keep some clustering so ObjectId-based chunking sees dense ranges too.
  - Shift all BSON datetime fields so document dates stay consistent with the generated ObjectId time.
  - Use multiprocessing and unordered bulk inserts for better throughput.

Example:
  export MONGO_URI='mongodb+srv://user:password@cluster.example.mongodb.net/'

  python loadtest_objectid_time_generator.py \
    --db sample_airbnb \
    --source listingsAndReviews \
    --target listingsAndReviews_loadtest \
    --target-gb 500 \
    --processes 6 \
    --batch-size 000 \
    --drop-target

Notes:
  - Do not hardcode credentials in this file.
  - For fastest loading, load into a target collection without secondary indexes.
  - Create secondary indexes after the data load.
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import sys
import time
import traceback
import multiprocessing as mp
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from bson import BSON, ObjectId
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from pymongo.errors import BulkWriteError


DEFAULT_COMMON_ANCHOR_FIELDS = [
    "createdAt",
    "created_at",
    "creationDate",
    "created",
    "date",
    "last_scraped",
    "calendar_last_scraped",
    "first_review",
    "last_review",
    "updatedAt",
    "updated_at",
]


@dataclass(frozen=True)
class RunConfig:
    mongo_uri: str
    db_name: str
    source_collection: str
    target_collection: str
    target_gb: Optional[float]
    multiplier: Optional[int]
    source_limit: int
    processes: int
    batch_size: int
    years_back: int
    mode: str
    even_weight: float
    cluster_weight: float
    recent_weight: float
    recent_days: int
    cluster_every_days: int
    cluster_jitter_days: int
    anchor_field: Optional[str]
    add_load_metadata: bool
    write_concern_w: int
    drop_target: bool
    dry_run: bool
    progress_every: int


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def normalize_datetime(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def get_nested_value(doc: dict[str, Any], dotted_path: str) -> Any:
    current: Any = doc
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def iter_datetimes(value: Any) -> Iterable[dt.datetime]:
    if isinstance(value, dt.datetime):
        yield normalize_datetime(value)
    elif isinstance(value, dict):
        for child in value.values():
            yield from iter_datetimes(child)
    elif isinstance(value, list):
        for child in value:
            yield from iter_datetimes(child)


def find_anchor_datetime(doc: dict[str, Any], anchor_field: Optional[str]) -> Optional[dt.datetime]:
    """
    Pick the source document's anchor time.

    The generated document's ObjectId timestamp becomes the new anchor time.
    Every datetime field shifts by:

        generated_objectid_time - source_anchor_time

    Priority:
      1. Explicit --anchor-field if provided.
      2. Source _id generation time if source _id is ObjectId.
      3. Common date fields such as last_scraped, createdAt, first_review.
      4. Median datetime found anywhere in the document.
      5. None.
    """
    if anchor_field:
        value = get_nested_value(doc, anchor_field)
        if isinstance(value, ObjectId):
            return normalize_datetime(value.generation_time)
        if isinstance(value, dt.datetime):
            return normalize_datetime(value)
        raise ValueError(
            f"--anchor-field '{anchor_field}' was provided but was not a datetime/ObjectId "
            f"for source document with _id={doc.get('_id')!r}"
        )

    source_id = doc.get("_id")
    if isinstance(source_id, ObjectId):
        return normalize_datetime(source_id.generation_time)

    for field in DEFAULT_COMMON_ANCHOR_FIELDS:
        value = get_nested_value(doc, field)
        if isinstance(value, dt.datetime):
            return normalize_datetime(value)

    all_dates = sorted(iter_datetimes(doc))
    if all_dates:
        return all_dates[len(all_dates) // 2]

    return None


def clone_and_shift_dates(value: Any, offset: dt.timedelta) -> Any:
    """
    Deep-copy only the containers we walk while shifting all datetime values.
    This avoids mutating the source document.
    """
    if isinstance(value, dt.datetime):
        return normalize_datetime(value) + offset

    if isinstance(value, dict):
        return {
            key: clone_and_shift_dates(child, offset)
            for key, child in value.items()
            if key != "_id"
        }

    if isinstance(value, list):
        return [clone_and_shift_dates(child, offset) for child in value]

    return value


def make_object_id_from_timestamp(ts_seconds: int, run_id: int, worker_id: int, local_seq: int) -> ObjectId:
    """
    Build an ObjectId where the first 4 bytes are the chosen timestamp.

    Layout:
      4 bytes timestamp
      2 bytes run_id
      2 bytes worker_id
      4 bytes local sequence

    This gives deterministic uniqueness as long as a worker writes fewer than
    4,294,967,296 documents in one run.
    """
    if not 0 <= ts_seconds <= 0xFFFFFFFF:
        raise ValueError(f"ObjectId timestamp seconds out of range: {ts_seconds}")
    if not 0 <= run_id <= 0xFFFF:
        raise ValueError(f"run_id out of range: {run_id}")
    if not 0 <= worker_id <= 0xFFFF:
        raise ValueError(f"worker_id out of range: {worker_id}")
    if not 0 <= local_seq <= 0xFFFFFFFF:
        raise ValueError(f"local_seq out of range: {local_seq}")

    raw = (
        ts_seconds.to_bytes(4, "big")
        + run_id.to_bytes(2, "big")
        + worker_id.to_bytes(2, "big")
        + local_seq.to_bytes(4, "big")
    )
    return ObjectId(raw)


def build_cluster_centers(start: dt.datetime, end: dt.datetime, every_days: int) -> list[dt.datetime]:
    centers: list[dt.datetime] = []
    current = start + dt.timedelta(days=max(every_days // 2, 1))

    while current < end:
        centers.append(current)
        current += dt.timedelta(days=every_days)

    if not centers:
        centers.append(start + (end - start) / 2)

    return centers


def choose_target_datetime(
    *,
    global_seq: int,
    total_docs: int,
    start: dt.datetime,
    end: dt.datetime,
    mode: str,
    rng: random.Random,
    even_weight: float,
    cluster_weight: float,
    recent_weight: float,
    recent_days: int,
    cluster_centers: list[dt.datetime],
    cluster_jitter_days: int,
) -> dt.datetime:
    """
    Pick the ObjectId generation time.

    Modes:
      even:
        Smooth spread over the entire range.

      clustered:
        Dense areas around periodic cluster centers.

      recent:
        Random within the recent window.

      hybrid:
        Mixture of even, clustered, and recent-heavy.
    """
    total_docs_safe = max(total_docs - 1, 1)
    span_seconds = int((end - start).total_seconds())

    def pick_even() -> dt.datetime:
        offset = int((span_seconds * global_seq) / total_docs_safe)
        return start + dt.timedelta(seconds=offset)

    def pick_clustered() -> dt.datetime:
        base = rng.choice(cluster_centers)
        jitter = rng.randint(-cluster_jitter_days * 86400, cluster_jitter_days * 86400)
        candidate = base + dt.timedelta(seconds=jitter)
        return min(max(candidate, start), end)

    def pick_recent() -> dt.datetime:
        recent_start = max(start, end - dt.timedelta(days=recent_days))
        recent_span_seconds = max(int((end - recent_start).total_seconds()), 1)
        return recent_start + dt.timedelta(seconds=rng.randint(0, recent_span_seconds))

    if mode == "even":
        return pick_even()

    if mode == "clustered":
        return pick_clustered()

    if mode == "recent":
        return pick_recent()

    if mode != "hybrid":
        raise ValueError(f"Unsupported mode: {mode}")

    total_weight = even_weight + cluster_weight + recent_weight
    if total_weight <= 0:
        raise ValueError("At least one distribution weight must be greater than 0")

    roll = rng.random() * total_weight

    if roll < even_weight:
        return pick_even()

    if roll < even_weight + cluster_weight:
        return pick_clustered()

    return pick_recent()


def estimate_avg_bson_size(docs: list[dict[str, Any]], sample_size: int = 500) -> float:
    sample = docs[: min(len(docs), sample_size)]
    if not sample:
        raise ValueError("Cannot estimate size from empty source docs")
    return sum(len(BSON.encode(doc)) for doc in sample) / len(sample)


def create_client(uri: str) -> MongoClient:
    return MongoClient(
        uri,
        tz_aware=True,
        tzinfo=dt.timezone.utc,
        connect=True,
        serverSelectionTimeoutMS=30_000,
    )


def load_source_docs(config: RunConfig) -> list[dict[str, Any]]:
    client = create_client(config.mongo_uri)
    try:
        coll = client[config.db_name][config.source_collection]
        cursor = coll.find({})
        if config.source_limit > 0:
            cursor = cursor.limit(config.source_limit)
        return list(cursor)
    finally:
        client.close()


def split_work(total_docs: int, processes: int) -> list[tuple[int, int, int]]:
    """
    Return tuples:
      worker_id, start_global_seq, docs_for_worker
    """
    base = total_docs // processes
    remainder = total_docs % processes

    assignments: list[tuple[int, int, int]] = []
    start_seq = 0

    for worker_id in range(processes):
        docs_for_worker = base + (1 if worker_id < remainder else 0)
        assignments.append((worker_id, start_seq, docs_for_worker))
        start_seq += docs_for_worker

    return assignments


def worker_main(
    config: RunConfig,
    worker_id: int,
    start_global_seq: int,
    docs_for_worker: int,
    total_docs: int,
    run_id: int,
    start_iso: str,
    end_iso: str,
) -> int:
    """
    Each worker process opens its own MongoClient.
    Do not share MongoClient across processes.
    """
    start = dt.datetime.fromisoformat(start_iso)
    end = dt.datetime.fromisoformat(end_iso)

    rng = random.Random((run_id * 1_000_003) + worker_id)
    cluster_centers = build_cluster_centers(start, end, config.cluster_every_days)

    client = create_client(config.mongo_uri)

    try:
        db = client[config.db_name]
        target = db.get_collection(
            config.target_collection,
            write_concern=WriteConcern(w=config.write_concern_w),
        )

        source_docs = list(db[config.source_collection].find({}))
        if config.source_limit > 0:
            source_docs = source_docs[: config.source_limit]

        if not source_docs:
            raise RuntimeError("No source documents found")

        source_count = len(source_docs)

        # Precompute anchor dates once per worker.
        source_anchors: list[Optional[dt.datetime]] = [
            find_anchor_datetime(doc, config.anchor_field) for doc in source_docs
        ]

        batch: list[dict[str, Any]] = []
        inserted = 0

        for local_seq in range(docs_for_worker):
            global_seq = start_global_seq + local_seq

            source_index = global_seq % source_count
            original_doc = source_docs[source_index]
            source_anchor = source_anchors[source_index]

            target_dt = choose_target_datetime(
                global_seq=global_seq,
                total_docs=total_docs,
                start=start,
                end=end,
                mode=config.mode,
                rng=rng,
                even_weight=config.even_weight,
                cluster_weight=config.cluster_weight,
                recent_weight=config.recent_weight,
                recent_days=config.recent_days,
                cluster_centers=cluster_centers,
                cluster_jitter_days=config.cluster_jitter_days,
            )

            target_ts = int(target_dt.timestamp())

            if source_anchor is None:
                # No source dates found. Copy document shape and only replace _id.
                # This is rare for sample datasets but keeps the script safe.
                doc = {
                    key: value
                    for key, value in original_doc.items()
                    if key != "_id"
                }
            else:
                offset = target_dt - source_anchor
                doc = clone_and_shift_dates(original_doc, offset)

            doc["_id"] = make_object_id_from_timestamp(
                ts_seconds=target_ts,
                run_id=run_id,
                worker_id=worker_id,
                local_seq=local_seq,
            )

            if config.add_load_metadata:
                doc["_load_test"] = {
                    "run_id": run_id,
                    "worker_id": worker_id,
                    "global_seq": global_seq,
                    "source_index": source_index,
                    "objectid_time": target_dt,
                    "mode": config.mode,
                }

            batch.append(doc)

            if len(batch) >= config.batch_size:
                try:
                    target.insert_many(
                        batch,
                        ordered=False,
                        bypass_document_validation=True,
                    )
                except BulkWriteError as exc:
                    print(f"Worker {worker_id}: bulk write error", file=sys.stderr)
                    print(exc.details, file=sys.stderr)
                    raise

                inserted += len(batch)
                batch.clear()

                if config.progress_every > 0 and inserted % config.progress_every == 0:
                    print(f"Worker {worker_id}: inserted {inserted:,}/{docs_for_worker:,}")

        if batch:
            try:
                target.insert_many(
                    batch,
                    ordered=False,
                    bypass_document_validation=True,
                )
            except BulkWriteError as exc:
                print(f"Worker {worker_id}: bulk write error", file=sys.stderr)
                print(exc.details, file=sys.stderr)
                raise

            inserted += len(batch)

        return inserted

    except Exception:
        print(f"Worker {worker_id} failed:", file=sys.stderr)
        traceback.print_exc()
        raise

    finally:
        client.close()


def parse_args() -> RunConfig:
    parser = argparse.ArgumentParser(
        description="Generate MongoDB load-test data with ObjectId timestamps spread across time."
    )

    parser.add_argument("--mongo-uri", default=os.environ.get("MONGO_URI"))
    parser.add_argument("--db", default="sample_airbnb")
    parser.add_argument("--source", default="listingsAndReviews")
    parser.add_argument("--target", default="listingsAndReviews_loadtest")

    size_group = parser.add_mutually_exclusive_group(required=True)
    size_group.add_argument("--target-gb", type=float)
    size_group.add_argument("--multiplier", type=int)

    parser.add_argument("--source-limit", type=int, default=0)
    parser.add_argument("--processes", type=int, default=6)
    parser.add_argument("--batch-size", type=int, default=1000)

    parser.add_argument("--years-back", type=int, default=10)
    parser.add_argument(
        "--mode",
        choices=["hybrid", "even", "clustered", "recent"],
        default="hybrid",
    )

    parser.add_argument("--even-weight", type=float, default=0.70)
    parser.add_argument("--cluster-weight", type=float, default=0.20)
    parser.add_argument("--recent-weight", type=float, default=0.10)
    parser.add_argument("--recent-days", type=int, default=365)

    parser.add_argument(
        "--cluster-every-days",
        type=int,
        default=90,
        help="Create cluster centers every N days across the full range.",
    )
    parser.add_argument(
        "--cluster-jitter-days",
        type=int,
        default=14,
        help="Clustered docs are placed within +/- this many days around a cluster center.",
    )

    parser.add_argument(
        "--anchor-field",
        default=None,
        help=(
            "Optional dotted path used as the source document anchor date, "
            "for example last_scraped or metadata.createdAt. "
            "If omitted, the script auto-detects."
        ),
    )

    parser.add_argument(
        "--no-load-metadata",
        action="store_true",
        help="Do not add the _load_test metadata field to generated documents.",
    )

    parser.add_argument("--write-concern-w", type=int, default=1)
    parser.add_argument("--drop-target", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--progress-every", type=int, default=100_000)

    args = parser.parse_args()

    if not args.mongo_uri:
        raise SystemExit("Missing MongoDB URI. Pass --mongo-uri or set MONGO_URI environment variable.")

    if args.processes < 1:
        raise SystemExit("--processes must be >= 1")

    if args.batch_size < 1:
        raise SystemExit("--batch-size must be >= 1")

    if args.years_back < 1:
        raise SystemExit("--years-back must be >= 1")

    if args.multiplier is not None and args.multiplier < 1:
        raise SystemExit("--multiplier must be >= 1")

    if args.target_gb is not None and args.target_gb <= 0:
        raise SystemExit("--target-gb must be greater than 0")

    if args.cluster_every_days < 1:
        raise SystemExit("--cluster-every-days must be >= 1")

    if args.cluster_jitter_days < 0:
        raise SystemExit("--cluster-jitter-days must be >= 0")

    return RunConfig(
        mongo_uri=args.mongo_uri,
        db_name=args.db,
        source_collection=args.source,
        target_collection=args.target,
        target_gb=args.target_gb,
        multiplier=args.multiplier,
        source_limit=args.source_limit,
        processes=args.processes,
        batch_size=args.batch_size,
        years_back=args.years_back,
        mode=args.mode,
        even_weight=args.even_weight,
        cluster_weight=args.cluster_weight,
        recent_weight=args.recent_weight,
        recent_days=args.recent_days,
        cluster_every_days=args.cluster_every_days,
        cluster_jitter_days=args.cluster_jitter_days,
        anchor_field=args.anchor_field,
        add_load_metadata=not args.no_load_metadata,
        write_concern_w=args.write_concern_w,
        drop_target=args.drop_target,
        dry_run=args.dry_run,
        progress_every=args.progress_every,
    )


def main() -> None:
    config = parse_args()

    source_docs = load_source_docs(config)
    if not source_docs:
        raise SystemExit("No source documents found.")

    avg_size = estimate_avg_bson_size(source_docs)

    if config.multiplier is not None:
        total_docs = len(source_docs) * config.multiplier
        target_size_gb = (total_docs * avg_size) / (1024 ** 3)
    else:
        total_docs = int((config.target_gb * 1024 ** 3) / avg_size)
        target_size_gb = config.target_gb

    if total_docs <= 0:
        raise SystemExit("Calculated total_docs <= 0. Check target size and source document size.")

    now = utc_now()
    start = now - dt.timedelta(days=config.years_back * 365)
    end = now

    print("")
    print("MongoDB ObjectId time-range load-test generator")
    print("------------------------------------------------")
    print(f"Database:               {config.db_name}")
    print(f"Source collection:      {config.source_collection}")
    print(f"Target collection:      {config.target_collection}")
    print(f"Source docs loaded:     {len(source_docs):,}")
    print(f"Estimated avg BSON:     {avg_size:,.0f} bytes")
    print(f"Estimated target size:  {target_size_gb:,.2f} GB")
    print(f"Estimated target docs:  {total_docs:,}")
    print(f"ObjectId time range:    {start.isoformat()} to {end.isoformat()}")
    print(f"Distribution mode:      {config.mode}")
    print(f"Processes:              {config.processes}")
    print(f"Batch size:             {config.batch_size}")
    print(f"Anchor field:           {config.anchor_field or 'auto'}")
    print(f"Drop target first:      {config.drop_target}")
    print(f"Dry run:                {config.dry_run}")
    print("")

    # Validate source anchors before running the expensive load.
    anchor_failures = 0
    for doc in source_docs[: min(len(source_docs), 100)]:
        try:
            find_anchor_datetime(doc, config.anchor_field)
        except Exception:
            anchor_failures += 1

    if anchor_failures:
        raise SystemExit(
            f"Anchor detection failed for {anchor_failures} of the first 100 docs. "
            f"Pass --anchor-field with a valid datetime field."
        )

    if config.dry_run:
        print("Dry run only. No inserts performed.")
        return

    client = create_client(config.mongo_uri)
    try:
        target = client[config.db_name][config.target_collection]
        if config.drop_target:
            print(f"Dropping target collection: {config.target_collection}")
            target.drop()
    finally:
        client.close()

    run_id = int(time.time()) & 0xFFFF
    assignments = split_work(total_docs, config.processes)

    print(f"Run ID: {run_id}")
    print("Starting workers...")
    started = time.time()

    # On macOS, spawn is the safe multiprocessing method.
    ctx = mp.get_context("spawn")

    with ctx.Pool(processes=config.processes) as pool:
        results = [
            pool.apply_async(
                worker_main,
                args=(
                    config,
                    worker_id,
                    start_seq,
                    docs_for_worker,
                    total_docs,
                    run_id,
                    start.isoformat(),
                    end.isoformat(),
                ),
            )
            for worker_id, start_seq, docs_for_worker in assignments
        ]

        total_inserted = 0
        for result in results:
            inserted = result.get()
            total_inserted += inserted
            print(f"Completed so far: {total_inserted:,}/{total_docs:,}")

    elapsed = time.time() - started
    docs_per_sec = total_inserted / elapsed if elapsed > 0 else 0
    mb_per_sec = ((total_inserted * avg_size) / (1024 ** 2)) / elapsed if elapsed > 0 else 0

    print("")
    print("Done.")
    print(f"Inserted docs:          {total_inserted:,}")
    print(f"Elapsed seconds:        {elapsed:,.1f}")
    print(f"Throughput docs/sec:    {docs_per_sec:,.0f}")
    print(f"Estimated MB/sec:       {mb_per_sec:,.2f}")
    print("")


if __name__ == "__main__":
    main()
