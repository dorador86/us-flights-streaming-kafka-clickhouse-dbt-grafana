import pandas as pd
import pyarrow.dataset as ds
import s3fs
import time
import multiprocessing as mp
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import clickhouse_connect

# ==========================================
# FULL INGESTION - STEADY-FLOW PATTERN (STABLE)
# ==========================================
WORKERS = 2
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
    
    global_producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 100,
        'batch.size': 1048576, 
        'compression.type': 'lz4',
        'acks': '0',
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

class S3FullIngestion:
    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=False)

    def run(self):
        print(f"\n🚀 LAUNCHING FINAL INGESTION (Steady-Flow | Workers: {WORKERS})")
        
        # 1. Nuclear Cleanup
        client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')
        tables = [
            "flights.flights_raw",
            "analytics.silver_flights_enriched",
            "analytics.gold_airline_daily",
            "analytics.gold_airport_daily"
        ]
        for table in tables:
            try:
                client.command(f"TRUNCATE TABLE {table}")
                print(f"✓ {table} truncated.")
            except: pass

        # 2. Files
        with open('.last_bucket_name', 'r') as f: bucket = f.read().strip()
        files = [
            f"{bucket}/raw/flights/year=2018/Combined_Flights_2018.parquet",
            f"{bucket}/raw/flights/year=2019/Combined_Flights_2019.parquet",
            f"{bucket}/raw/flights/year=2020/Combined_Flights_2020.parquet",
            f"{bucket}/raw/flights/year=2021/Combined_Flights_2021.parquet",
            f"{bucket}/raw/flights/year=2022/Combined_Flights_2022.parquet"
        ]

        total_global_sent = 0
        start_time = time.time()

        with mp.Pool(processes=WORKERS, initializer=init_worker) as pool:
            for s3_path in files:
                filename = s3_path.split('/')[-1]
                print(f"\n📂 Processing: {filename} ...")
                
                try:
                    dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
                except:
                    print(f"⚠️ Skipping {filename}: Not found")
                    continue

                cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
                        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                        'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                        'DestCityName', 'DestState', 'AirTime', 'Diverted']
                
                file_sent = 0
                for batch in dataset.to_batches(columns=cols, batch_size=100000):
                    df = batch.to_pandas()
                    df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
                    df['Cancelled'] = df['Cancelled'].astype(int)
                    df['Diverted'] = df['Diverted'].astype(int)
                    df = df.where(pd.notnull(df), None)
                    records = df.to_dict('records')
                    
                    chunk_size = len(records) // WORKERS
                    chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
                    
                    pool.map_async(producer_worker, chunks)
                    
                    batch_count = len(records)
                    file_sent += batch_count
                    total_global_sent += batch_count
                    
                    if total_global_sent % 500000 == 0:
                        elapsed = time.time() - start_time
                        print(f"📈 [Progress] {total_global_sent:,} records. Speed: {total_global_sent/elapsed:.0f} rec/s")

            print("\n⌛ Waiting for the system to process the last messages...")
            pool.close()
            pool.join()

        duration = time.time() - start_time
        print(f"\n🎉 FINAL INGESTION COMPLETED 🎉")
        print(f"📊 Total records: {total_global_sent:,}")
        print(f"⏱️ Total time: {duration/60:.2f} minutes")
        print(f"🚀 Average speed: {total_global_sent/duration:.0f} rec/s")

if __name__ == "__main__":
    S3FullIngestion().run()
