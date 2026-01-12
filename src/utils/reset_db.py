import subprocess
import sys

def run_clickhouse_query(query):
    cmd = [
        "docker", "exec", "clickhouse", 
        "clickhouse-client", "--user", "admin", "--password", "admin", 
        "--query", query
    ]
    try:
        subprocess.run(cmd, check=True)
        print(f"OK: {query}")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    print(">>> Reseting ClickHouse for clean benchmark...")
    # 1. Truncate target table
    run_clickhouse_query("TRUNCATE TABLE flights.flights_raw")
    
    # 2. Recreate/Clear MV (Truncating raw usually clears the pipe if MV is TO)
    # But just to be safe, we can delete any leftover in queue if any (Kafka engine)
    # Actually, Kafka engine doesn't store data, but we might want to reset offsets.
    
    print(">>> Note: To reset Kafka offsets, you might need to change the group.id in ClickHouse or use kafka-consumer-groups tool.")
    print("Done.")
