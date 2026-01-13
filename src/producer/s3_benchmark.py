from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import pandas as pd
import pyarrow.dataset as ds
import s3fs
import time
import os
import json
import argparse
import multiprocessing as mp

# ==========================================
# CONFIGURACIÓN FIJA PARA MÁXIMA ESTABILIDAD
# ==========================================
WORKERS = 2  # Optimizado para instancia de 2 núcleos
TOPIC = 'flights_avro_pro'
BOOTSTRAP_SERVERS = 'localhost:29092'
REGISTRY_URL = 'http://localhost:8081'

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

def producer_worker(records_chunk):
    """Worker de alto rendimiento: Serialización y Envío"""
    registry_client = SchemaRegistryClient({'url': REGISTRY_URL})
    serializer = AvroSerializer(registry_client, SCHEMA_STR, lambda obj, ctx: obj)
    
    producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 150,
        'batch.size': 2097152,  # 2MB
        'compression.type': 'lz4',
        'acks': '1',
        'queue.buffering.max.messages': 1000000,
        'queue.buffering.max.kbytes': 1048576,
        'message.max.bytes': 2097152
    })
    
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    count = 0
    poll = producer.poll
    produce = producer.produce
    
    for record in records_chunk:
        while True:
            try:
                val_bytes = serializer(record, ctx)
                produce(topic=TOPIC, value=val_bytes)
                break
            except BufferError:
                poll(0.05)
        
        count += 1
        if count % 20000 == 0:
            poll(0)
            
    producer.flush()
    return count

class S3Benchmark:
    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=False)

    def run(self, limit=500000):
        print(f"\n🚀 EJECUTANDO BENCHMARK ULTRA (Workers: {WORKERS})")
        
        # 1. Reset Database
        try:
            import clickhouse_connect
            client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')
            print(">>> Limpiando bases de datos para un test puro...")
            mt_tables = client.query("SELECT concat(database, '.', name) FROM system.tables WHERE engine = 'MergeTree' AND database IN ('flights', 'analytics')")
            for row in mt_tables.result_rows:
                client.command(f"TRUNCATE TABLE {row[0]}")
            print("✓ ClickHouse reseteado.")
        except Exception as e:
            print(f"! Aviso: No se pudo resetear la BBDD: {e}")

        # 2. Leer datos
        with open('.last_bucket_name', 'r') as f:
            bucket = f.read().strip()
        s3_path = f"{bucket}/raw/flights/year=2022/Combined_Flights_2022.parquet"
        
        print(f"Cargando {limit:,} filas desde S3...")
        dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
        cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
                'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                'DestCityName', 'DestState', 'AirTime', 'Diverted']
        
        df = dataset.head(limit, columns=cols).to_pandas()

        # 3. Pre-procesado Vectorizado
        print("Pre-procesando datos (Optimización de CPU)...")
        df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
        df['Cancelled'] = df['Cancelled'].astype(int)
        df['Diverted'] = df['Diverted'].astype(int)
        df = df.where(pd.notnull(df), None)
        records = df.to_dict('records')
        
        # 4. Paralelizar
        chunk_size = len(records) // WORKERS
        chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
        
        print(f"Lanzando {WORKERS} workers paralelos...")
        start_time = time.time()
        
        with mp.Pool(processes=WORKERS) as pool:
            results = pool.map(producer_worker, chunks)
            total_sent = sum(results)
            
        duration = time.time() - start_time
        avg_rps = total_sent / duration
        
        print(f"\n✅ BENCHMARK COMPLETADO: {total_sent:,} registros en {duration:.2f}s")
        print(f"🔥 RENDIMIENTO FINAL: {avg_rps:.0f} rec/s 🔥")
        
        # Log de métricas
        try:
            client.command(f"""
                INSERT INTO analytics.ingestion_benchmarks (test_id, format, records, duration_seconds, avg_rps, timestamp)
                VALUES ('ultra_bench_{int(time.time())}', 'parallel_ultra', {total_sent}, {duration}, {avg_rps}, now())
            """)
            print("✅ Métricas registradas en ClickHouse.")
        except: pass

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=500000)
    args = parser.parse_args()
    
    bench = S3Benchmark()
    bench.run(limit=args.limit)
