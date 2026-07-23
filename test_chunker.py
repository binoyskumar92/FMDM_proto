import argparse
import json
from bson.objectid import ObjectId
from pymongo import MongoClient
from etl_benchmark import plan_chunks, initialize_bookkeeping


def fmt_bound(value) -> str:
    """Human-readable bound regardless of _id type."""
    if value is None:
        return "<open>"
    if isinstance(value, ObjectId):
        return f"{value} ({value.generation_time.strftime('%Y-%m-%d %H:%M:%S')})"
    return str(value)


def test_only_chunking(config_file_path: str):
    with open(config_file_path, "r") as f:
        job_config = json.load(f)

    print(f"--- Chunk Planner Test: {job_config['job_id']} ---")
    print(f"Mode            : {job_config['mode']}")
    print(f"Max docs/chunk  : {job_config['max_docs_per_chunk']}")
    print()

    bk_client = MongoClient(job_config["bookkeeping_uri"])
    db = bk_client["masking_control"]

    print("Clearing previous bookkeeping entries for this job...")
    db.jobs.delete_many({"job_id": job_config["job_id"]})
    db.chunks.delete_many({"job_id": job_config["job_id"]})

    initialize_bookkeeping(job_config["bookkeeping_uri"], job_config)

    print("Running chunk planner...\n")
    plan_chunks(job_config)

    generated_chunks = list(
        db.chunks.find({"job_id": job_config["job_id"]}).sort("chunk_sequence", 1)
    )

    total = len(generated_chunks)
    print(f"{'=' * 52}")
    print(f"  Total chunks planned : {total}")
    if generated_chunks:
        counts = [c.get("docs_read_estimate", 0) for c in generated_chunks]
        print(f"  Min docs/chunk       : {min(counts):,}")
        print(f"  Max docs/chunk       : {max(counts):,}")
        print(f"  Avg docs/chunk       : {sum(counts) // len(counts):,}")
        planners = set(c.get("planner", "objectid") for c in generated_chunks)
        print(f"  Planner(s) used      : {', '.join(planners)}")
    print(f"{'=' * 52}\n")

    preview = generated_chunks[:10]
    print("--- First 10 chunks ---")
    for chunk in preview:
        planner = chunk.get("planner", "objectid")
        print(f"  Seq {chunk['chunk_sequence']:>4}  [{planner}]")
        print(f"    Lower : {fmt_bound(chunk.get('lower_bound'))}")
        print(f"    Upper : {fmt_bound(chunk.get('upper_bound'))}")
        print(f"    Docs  : {chunk.get('docs_read_estimate', '?'):,}")
        print()

    if total > 10:
        print("... [middle chunks truncated] ...\n")
        print("--- Last 3 chunks ---")
        for chunk in generated_chunks[-3:]:
            planner = chunk.get("planner", "objectid")
            print(f"  Seq {chunk['chunk_sequence']:>4}  [{planner}]")
            print(f"    Lower : {fmt_bound(chunk.get('lower_bound'))}")
            print(f"    Upper : {fmt_bound(chunk.get('upper_bound'))}")
            print(f"    Docs  : {chunk.get('docs_read_estimate', '?'):,}")
            print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the FMDM chunk planner")
    parser.add_argument("--config", default="config.json", help="Path to config file")
    args = parser.parse_args()
    test_only_chunking(args.config)
