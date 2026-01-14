import pandas as pd
import pyarrow.dataset as ds
import s3fs
import time
import multiprocessing as mp
import psutil
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import clickhouse_connect

# ==========================================
# BENCHMARK GOLDEN-TURBO (80k-100k TARGET)
# ==========================================
WORKERS = 2 # 1 per physical core = maximum real efficiency
BOOTSTRAP_SERVERS = 'localhost:29092'
REGISTRY_URL = 'http://localhost:8081'
TOPIC = 'flights_avro_pro'

SCHEMA_STR = """
{
  "type": "record",
  "name": "FlightRecord",
  "namespace": "com.flights",
  "fields": [
    {"name": "FlightDate", "type": "long"},
    {"name": "Airline", "type": "string"},
    {"name": "Tail_Number", "type": ["null", "string"], "default": null},
    {"name": "Origin", "type": "string"},
    {"name": "Dest", "type": "string"},
    {"name": "Cancelled", "type": "int"},
    {"name": "DepDelay", "type": ["null", "float"], "default": null},
    {"name": "ArrDelay", "type": ["null", "float"], "default": null},
    {"name": "Distance", "type": "float"},
    {"name": "Year", "type": "int"},
    {"name": "Quarter", "type": "int"},
    {"name": "Month", "type": "int"},
    {"name": "DayofMonth", "type": "int"},
    {"name": "DayOfWeek", "type": "int"},
    {"name": "Marketing_Airline_Network", "type": "string"},
    {"name": "OriginCityName", "type": "string"},
    {"name": "OriginState", "type": "string"},
    {"name": "DestCityName", "type": "string"},
    {"name": "DestState", "type": "string"},
    {"name": "AirTime", "type": ["null", "float"], "default": null},
    {"name": "Diverted", "type": "int"}
  ]
}
"""

def init_worker():
    global global_producer, global_serializer, global_ctx
    registry_client = SchemaRegistryClient({'url': REGISTRY_URL})
    global_serializer = AvroSerializer(registry_client, SCHEMA_STR, lambda obj, ctx: obj)
    global_ctx = SerializationContext(TOPIC, MessageField.VALUE)
    
    # STEADY-FLOW MODE (Maximize consistency)
    global_producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 100,
        'batch.size': 1048576, 
        'compression.type': 'lz4',
        'acks': '0', # We don't wait for the broker to avoid slowing down the flow
        'queue.buffering.max.messages': 1000000,
        'message.max.bytes': 4194304
    })

def producer_worker(chunk):
    if not chunk: return 0
    count = 0
    produce = global_producer.produce
    serialize = global_serializer
    ctx = global_ctx
    
    for record in chunk:
        try:
            produce(topic=TOPIC, value=serialize(record, ctx))
            count += 1
        except BufferError:
            global_producer.poll(0)
    
    global_producer.poll(0)
    return count

def run_performance_test():
    limit = 5000000 
    print(f"\n🚀 LAUNCHING STEADY-FLOW MODE (Limit: {limit:,})")
    
    client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')
    client.command("TRUNCATE TABLE flights.flights_raw")
    # Optional: client.command("SYSTEM STOP MERGES") # If we want to see the pure peak
    
    s3 = s3fs.S3FileSystem(anon=False)
    with open('.last_bucket_name', 'r') as f: bucket = f.read().strip()
    s3_path = f"{bucket}/raw/flights/year=2022/Combined_Flights_2022.parquet"
    
    dataset = ds.dataset(s3_path, filesystem=s3, format="parquet")
    cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
            'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
            'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
            'DestCityName', 'DestState', 'AirTime', 'Diverted']
    
    total_sent = 0
    start_time = time.time()
    
    with mp.Pool(processes=WORKERS, initializer=init_worker) as pool:
        # 100k blocks: Much lighter for the main thread
        for batch in dataset.to_batches(columns=cols, batch_size=100000):
            if total_sent >= limit: break
            
            df = batch.to_pandas()
            df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
            df['Cancelled'] = df['Cancelled'].astype(int)
            df['Diverted'] = df['Diverted'].astype(int)
            df = df.where(pd.notnull(df), None)
            records = df.to_dict('records')
            
            # Divide into 2 workers
            chunk_size = len(records) // WORKERS
            chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
            
            pool.map_async(producer_worker, chunks)
            total_sent += len(records)
            
            elapsed = time.time() - start_time
            if total_sent % 500000 == 0:
                print(f"📈 [Test] {total_sent:,} queued. Speed: {total_sent/elapsed:.0f} rec/s")
        
        pool.close()
        pool.join()

    total_duration = time.time() - start_time
    print(f"\n✅ FINAL RESULT: {total_sent:,} records in {total_duration:.1f}s")
    print(f"🔥 AVERAGE SPEED: {total_sent/total_duration:.0f} rec/s 🔥")

if __name__ == "__main__":
    run_performance_test()
