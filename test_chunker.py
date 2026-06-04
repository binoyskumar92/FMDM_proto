import json
from pymongo import MongoClient
# Import the planning and initialization functions from your main file
from etl_benchmark import plan_chunks, initialize_bookkeeping

def test_only_chunking(config_file_path: str):
    # 1. Load your configuration
    with open(config_file_path, 'r') as f:
        job_config = json.load(f)
    
    print(f"--- Isolating Chunk Planner for Job: {job_config['job_id']} ---")
    print(f"Source Collection: {job_config['source_db']}.{job_config['source_collection']}")
    print(f"Target Max Docs Per Chunk Threshold: {job_config['max_docs_per_chunk']}")
    print(f"Initial Window Size: {job_config['chunk_window_hours']} hour(s)\n")
    
    # 2. Connect to Bookkeeping DB to drop old test chunks for a clean run
    bk_client = MongoClient(job_config['bookkeeping_uri'])
    db = bk_client["masking_control"]
    
    print("Clearing previous bookkeeping entries for this job...")
    db.jobs.delete_many({"job_id": job_config["job_id"]})
    db.chunks.delete_many({"job_id": job_config["job_id"]})
    
    # 3. Run the initialization and the planner
    initialize_bookkeeping(job_config['bookkeeping_uri'], job_config)
    
    print("Executing chunk planner logic...")
    plan_chunks(job_config)
    print("Planning phase completed.\n")
    
    # 4. Fetch and inspect the generated chunks from the database
    generated_chunks = list(db.chunks.find({"job_id": job_config["job_id"]}).sort("chunk_sequence", 1))
    
    print(f"==================================================")
    print(f"PLANNING RESULTS: Generated {len(generated_chunks)} total chunks.")
    print(f"==================================================")
    
    # Print the first 10 chunks to see the data distribution shape
    print("\n--- Inspecting First 10 Chunks ---")
    for chunk in generated_chunks[:10]:
        print(f"Seq {chunk['chunk_sequence']}:")
        print(f"  Lower Bound OID : {chunk['lower_bound']} ({chunk['lower_bound'].generation_time})")
        print(f"  Upper Bound OID : {chunk['upper_bound']} ({chunk['upper_bound'].generation_time})")
        print(f"  Est. Doc Count  : {chunk['bytes_read_estimate']}")
        print("-" * 40)
        
    # If there are many chunks, print the last few as well
    if len(generated_chunks) > 10:
        print("\n... [Truncated middle chunks] ...\n")
        print("--- Inspecting Last 3 Chunks ---")
        for chunk in generated_chunks[-3:]:
            print(f"Seq {chunk['chunk_sequence']}:")
            print(f"  Lower Bound OID : {chunk['lower_bound']} ({chunk['lower_bound'].generation_time})")
            print(f"  Upper Bound OID : {chunk['upper_bound']} ({chunk['upper_bound'].generation_time})")
            print(f"  Est. Doc Count  : {chunk['bytes_read_estimate']}")
            print("-" * 40)

if __name__ == "__main__":
    # Point this to your config.json file
    test_only_chunking("config.json")