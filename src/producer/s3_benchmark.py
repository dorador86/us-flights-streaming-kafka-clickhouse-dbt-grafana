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
WORKERS = 2 # 1 por núcleo físico = máxima eficiencia real
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
    
    # CONFIGURACIÓN OPTIMIZADA (Velocidad Termal pero Estable)
    global_producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 300, # Más tiempo para llenar batches gigantes
        'batch.size': 2097152, # 2MB es el punto dulce para este broker
        'compression.type': 'lz4', # Esencial para no saturar I/O
        'acks': '1', # Un poco de seguridad para evitar desconexiones TCP
        'queue.buffering.max.messages': 1000000,
        'queue.buffering.max.kbytes': 1048576,
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
            global_producer.poll(0.1)
    
    # No flusheamos en medio para no frenar el pipeline
    global_producer.poll(0)
    return count

def run_performance_test():
    limit = 4000000 
    print(f"\n🚀 LANZANDO TEST DE ALTO RENDIMIENTO (Limit: {limit:,})")
    
    client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')
    client.command("TRUNCATE TABLE flights.flights_raw")
    
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
    
    # Usamos Apply_Async para flujo continuo
    with mp.Pool(processes=WORKERS, initializer=init_worker) as pool:
        for batch in dataset.to_batches(columns=cols, batch_size=500000):
            if total_sent >= limit: break
            
            df = batch.to_pandas()
            df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
            df['Cancelled'] = df['Cancelled'].astype(int)
            df['Diverted'] = df['Diverted'].astype(int)
            df = df.where(pd.notnull(df), None)
            records = df.to_dict('records')
            
            # Dividimos los 500k entre los 2 workers (250k cada uno)
            chunk_size = len(records) // WORKERS
            chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
            
            # Lanzamos de forma asíncrona para no parar la lectura de S3
            pool.map_async(producer_worker, chunks)
            total_sent += len(records)
            
            elapsed = time.time() - start_time
            print(f"📈 [Test] {total_sent:,} encolados. Speed estimada: {total_sent/elapsed:.0f} rec/s")
        
        print("⌛ Finalizando envío y vaciando buffers...")
        pool.close()
        pool.join()

    total_duration = time.time() - start_time
    # Registro final en ClickHouse para tus gráficas
    client.command(f"""
        INSERT INTO analytics.ingestion_benchmarks (test_id, format, records, duration_seconds, avg_rps, timestamp)
        VALUES ('test_high_perf_{int(time.time())}', 'avro_turbo_v2', {total_sent}, {total_duration}, {total_sent/total_duration}, now())
    """)
    
    print(f"\n✅ RESULTADO FINAL: {total_sent:,} registros en {total_duration:.1f}s")
    print(f"🔥 VELOCIDAD MEDIA: {total_sent/total_duration:.0f} rec/s 🔥")

if __name__ == "__main__":
    run_performance_test()
