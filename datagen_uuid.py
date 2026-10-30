"""
datagen_uuid.py — seeds test collections with non-ObjectId _ids for testing
                  the $bucketAuto chunk planner.

Creates two collections:
  - test_string_ids  : string _ids  (e.g. "user_000042")
  - test_uuid_ids    : UUID string _ids  (e.g. "f3a2bc11-...")

Usage:
  python datagen_uuid.py --uri <mongo_uri> --db <db_name> --docs 50000
"""
import argparse
import uuid
import random
from pymongo import MongoClient

BATCH_SIZE = 2000


def seed_string_ids(db, n_docs: int):
    coll = db["test_string_ids"]
    coll.drop()
    print(f"Seeding test_string_ids with {n_docs:,} docs...")
    batch = []
    for i in range(n_docs):
        batch.append({
            "_id": f"user_{i:08d}",
            "first_name": f"Name{i}",
            "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
            "account": {"account_number": f"ACC{random.randint(100000,999999)}"},
            "score": random.random(),
        })
        if len(batch) >= BATCH_SIZE:
            coll.insert_many(batch)
            batch = []
    if batch:
        coll.insert_many(batch)
    print(f"  Done. {coll.count_documents({}):,} docs in test_string_ids.")


def seed_uuid_ids(db, n_docs: int):
    coll = db["test_uuid_ids"]
    coll.drop()
    print(f"Seeding test_uuid_ids with {n_docs:,} docs...")
    batch = []
    for i in range(n_docs):
        batch.append({
            "_id": str(uuid.uuid4()),
            "first_name": f"Name{i}",
            "ssn": f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}",
            "account": {"account_number": f"ACC{random.randint(100000,999999)}"},
            "score": random.random(),
        })
        if len(batch) >= BATCH_SIZE:
            coll.insert_many(batch)
            batch = []
    if batch:
        coll.insert_many(batch)
    print(f"  Done. {coll.count_documents({}):,} docs in test_uuid_ids.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True, help="MongoDB connection string")
    parser.add_argument("--db", default="fmdm_test", help="Database name")
    parser.add_argument("--docs", type=int, default=50000, help="Docs per collection")
    args = parser.parse_args()

    client = MongoClient(args.uri)
    db = client[args.db]

    seed_string_ids(db, args.docs)
    seed_uuid_ids(db, args.docs)

    print("\nDone. Add these to your config.json under 'collections' to test the planner:")
    print("""
  {
    "source_collection": "test_string_ids",
    "dest_collection": "test_string_ids_dest",
    "drop_destination_collection_before_load": true,
    "masking_fields": ["ssn", "account.account_number", "first_name"]
  },
  {
    "source_collection": "test_uuid_ids",
    "dest_collection": "test_uuid_ids_dest",
    "drop_destination_collection_before_load": true,
    "masking_fields": ["ssn", "account.account_number", "first_name"]
  }
""")
