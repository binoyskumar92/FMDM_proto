import random
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

# --- CONFIGURATION ---
MONGO_URI = "mongodb+srv://mongoadmin:passwordone@cluster0.hvabt.mongodb.net/"
DB_NAME = "sample_airbnb"
SOURCE_COLLECTION = "listingsAndReviews_loadtest"
TARGET_COLLECTION = "listingsAndReviews_loadtest_push"

MULTIPLIER = 50           # How many times to duplicate the dataset
BATCH_SIZE = 3000         # Number of docs to insert per batch
SHIFT_DAYS_RANGE = (-30, 30) # Range of days to randomly shift existing date fields

# Define cluster points for ObjectId generation (e.g., specific days)
# The script will tightly group the generated ObjectIds around these times.
CLUSTER_BASE_DATES = [
    datetime.datetime(2023, 1, 15),
    datetime.datetime(2023, 4, 20),
    datetime.datetime(2023, 8, 10),
    datetime.datetime(2024, 11, 5)
]

def shift_datetime_fields(doc, timedelta_offset):
    """
    Recursively searches through a document and shifts any datetime objects 
    by the specified timedelta to keep the organic data looking varied.
    """
    if isinstance(doc, dict):
        for k, v in doc.items():
            doc[k] = shift_datetime_fields(v, timedelta_offset)
        return doc
    elif isinstance(doc, list):
        return [shift_datetime_fields(item, timedelta_offset) for item in doc]
    elif isinstance(doc, datetime.datetime):
        return doc + timedelta_offset
    else:
        return doc

def generate_clustered_objectid():
    """
    Generates an ObjectId clustered around one of the predefined base dates.
    Adds a small random variance (0 to 60 minutes) to simulate a burst of inserts.
    """
    base_date = random.choice(CLUSTER_BASE_DATES)
    # Add a random offset of up to 60 minutes from the base date to create a tight cluster
    random_seconds_offset = random.randint(0, 3600) 
    cluster_time = base_date + datetime.timedelta(seconds=random_seconds_offset)
    
    # Generate an ObjectId using the specific datetime
    # We use a random 8-byte hex string for the rest of the ObjectId to ensure uniqueness
    dummy_id = ObjectId.from_datetime(cluster_time)
    
    # ObjectId is 12 bytes: 4 bytes timestamp + 8 bytes random payload (in modern drivers)
    # We keep the first 8 hex characters (4 bytes of time) and randomize the rest.
    timestamp_hex = str(dummy_id)[:8]
    random_payload = ''.join(random.choices('0123456789abcdef', k=16))
    
    return ObjectId(timestamp_hex + random_payload)

def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    source_coll = db[SOURCE_COLLECTION]
    target_coll = db[TARGET_COLLECTION]

    print(f"Fetching source documents from {SOURCE_COLLECTION}...")
    # Pulling all docs into memory. If the source collection is too large, 
    # you may want to limit this (e.g., .limit(10000)) or iterate with a cursor.
    source_docs = list(source_coll.find({}))
    
    if not source_docs:
        print("No documents found in the source collection!")
        return

    print(f"Loaded {len(source_docs)} documents. Multiplying by {MULTIPLIER}...")

    total_inserted = 0
    batch = []

    for i in range(MULTIPLIER):
        print(f"Starting iteration {i + 1}/{MULTIPLIER}...")
        
        for original_doc in source_docs:
            # Create a deep copy to avoid modifying the original in-memory document
            new_doc = dict(original_doc)
            
            # 1. Generate a new, time-clustered ObjectId
            new_doc['_id'] = generate_clustered_objectid()
            
            # 2. Shift date fields organically
            days_to_shift = random.randint(SHIFT_DAYS_RANGE[0], SHIFT_DAYS_RANGE[1])
            time_offset = datetime.timedelta(days=days_to_shift)
            new_doc = shift_datetime_fields(new_doc, time_offset)
            
            # Add to batch
            batch.append(new_doc)
            
            # 3. Bulk Insert
            if len(batch) >= BATCH_SIZE:
                target_coll.insert_many(batch)
                total_inserted += len(batch)
                batch = []
                
    # Insert any remaining documents in the final batch
    if batch:
        target_coll.insert_many(batch)
        total_inserted += len(batch)

    print(f"Success! Inserted {total_inserted} documents into {TARGET_COLLECTION}.")

if __name__ == "__main__":
    main()