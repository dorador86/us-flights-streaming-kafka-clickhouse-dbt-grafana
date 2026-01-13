import pandas as pd
import pyarrow.dataset as ds
import s3fs
import time
import argparse
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

# ==========================================
# BENCHMARK AVRO OPTIMIZADO (SINGLE THREAD)
# ==========================================
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

def run_avro_optimized_benchmark(limit=500000):
    print(f"\n🚀 LANZANDO BENCHMARK AVRO SUPER-OPTIMIZADO (Single Producer)")
    
    # 1. Configuración Kafka "High Throughput" para AVRO
    producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'linger.ms': 300,             # Clave: Esperar para batching masivo
        'batch.size': 4194304,        # 4 MB
        'compression.type': 'lz4',    # Clave: Reducir tamaño en red
        'acks': '1',
        'queue.buffering.max.messages': 2000000,
        'queue.buffering.max.kbytes': 2097152,
        'message.send.max.retries': 0,
    })
    
    registry_client = SchemaRegistryClient({'url': REGISTRY_URL})
    serializer = AvroSerializer(registry_client, SCHEMA_STR, lambda obj, ctx: obj)
    ctx = SerializationContext(TOPIC, MessageField.VALUE)
    
    # 2. Cargar datos en RAM
    s3 = s3fs.S3FileSystem(anon=False)
    try:
        with open('.last_bucket_name', 'r') as f: bucket = f.read().strip()
        s3_path = f"{bucket}/raw/flights/year=2022/Combined_Flights_2022.parquet"
        print("Cargando datos en RAM...")
        dataset = ds.dataset(s3_path, filesystem=s3, format="parquet")
        cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
                'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                'DestCityName', 'DestState', 'AirTime', 'Diverted']
        
        df = dataset.head(limit, columns=cols).to_pandas()
        
        # Pre-conversión de tipos para descargar a la CPU durante el bucle
        df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
        df['Cancelled'] = df['Cancelled'].astype(int)
        df['Diverted'] = df['Diverted'].astype(int)
        df = df.where(pd.notnull(df), None)
        
        records = df.to_dict('records')
        print(f"✓ {len(records):,} registros en RAM preparados.")
    except Exception as e:
        print(f"Error cargando datos: {e}")
        return

    # 3. Ingesta a Velocidad AVRO Máxima
    print("🔥 INICIANDO INGESTA EN MONO-HILO OPTIMIZADO...")
    start_time = time.time()
    
    produce = producer.produce
    poll = producer.poll
    serialize = serializer # Pre-bind
    
    # Bucle simple: Sin IPC, sin multiprocessing, sin tonterías
    for i, record in enumerate(records):
        try:
            # Aquí es donde se gasta la CPU (Avro Serialization)
            # No podemos evitarlo, pero al ser 1 solo proceso, competimos menos por la caché L3
            produce(topic=TOPIC, value=serialize(record, ctx))
        except BufferError:
            poll(0.1)
            produce(topic=TOPIC, value=serialize(record, ctx))
            
        # Poll calculado para no frenar el batching
        if i % 10000 == 0:
            poll(0)

    # Flush final único
    producer.flush()
    
    duration = time.time() - start_time
    avg_rps = len(records) / duration
    
    print(f"\n✅ BENCHMARK FINALIZADO: {len(records):,} registros en {duration:.2f}s")
    print(f"⭐️ RENDIMIENTO FINAL: {avg_rps:.0f} rec/s ⭐️")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=1000000)
    args = parser.parse_args()
    run_avro_optimized_benchmark(limit=args.limit)
