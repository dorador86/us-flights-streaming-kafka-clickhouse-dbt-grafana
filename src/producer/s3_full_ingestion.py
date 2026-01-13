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
# CONFIGURACIÓN DE ALTO RENDIMIENTO (ULTRA)
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

def producer_worker(records_chunk):
    """Worker optimizado para serializar y enviar a Kafka en paralelo"""
    if not records_chunk: return 0
    
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
    
    for record in records_chunk:
        while True:
            try:
                val_bytes = serializer(record, ctx)
                producer.produce(topic=TOPIC, value=val_bytes)
                break
            except BufferError:
                producer.poll(0.05)
        
        count += 1
        if count % 20000 == 0:
            producer.poll(0)
            
    producer.flush()
    return count

class S3FullIngestionUltra:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3 = s3fs.S3FileSystem(anon=False)
        self.ch_client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')

    def reset_database(self):
        """Limpia las tablas MergeTree para una ingesta limpia"""
        try:
            print(">>> Limpiando tablas de ClickHouse...")
            mt_tables = self.ch_client.query("SELECT concat(database, '.', name) FROM system.tables WHERE engine = 'MergeTree' AND database IN ('flights', 'analytics')")
            for row in mt_tables.result_rows:
                self.ch_client.command(f"TRUNCATE TABLE {row[0]}")
            print("✓ Base de datos lista.")
        except Exception as e:
            print(f"! Error reseteando DB: {e}")

    def process_file_parallel(self, s3_path, pool):
        """Procesa un archivo Parquet usando paralelismo de workers"""
        print(f"-> Procesando: {s3_path}")
        start_time = time.time()
        
        dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
        cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
                'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                'DestCityName', 'DestState', 'AirTime', 'Diverted']
        
        file_rows = 0
        # Cargamos el archivo por fragmentos grandes para no saturar memoria pero dar trabajo a los workers
        for batch in dataset.to_batches(columns=cols, batch_size=200000):
            df = batch.to_pandas()
            
            # Pre-procesado vectorizado (Rápido en el hilo principal)
            df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
            df['Cancelled'] = df['Cancelled'].astype(int)
            df['Diverted'] = df['Diverted'].astype(int)
            df = df.where(pd.notnull(df), None)
            
            records = df.to_dict('records')
            
            # Dividir el lote entre los workers
            chunk_size = len(records) // WORKERS
            if chunk_size == 0: chunks = [records]
            else: chunks = [records[i:i + chunk_size] for i in range(0, len(records), chunk_size)]
            
            # Mandar a los workers
            results = pool.map(producer_worker, chunks)
            file_rows += sum(results)
            
        duration = time.time() - start_time
        print(f"✓ {s3_path} completado: {file_rows:,} registros ({file_rows/duration:.0f} rec/s)")
        return file_rows

    def run(self):
        print(f"🚀 INGESTA MASIVA ULTRA (Paralelismo: {WORKERS} workers)")
        self.reset_database()
        
        search_path = f"{self.bucket_name}/raw/flights/"
        all_files = sorted(self.s3.glob(f"{search_path}**/*.parquet"))
        
        if not all_files:
            print("! No se encontraron archivos Parquet.")
            return

        grand_total = 0
        overall_start = time.time()
        
        # Usamos un Pool de procesos persistente para toda la ingesta
        with mp.Pool(processes=WORKERS) as pool:
            for file_path in all_files:
                rows = self.process_file_parallel(file_path, pool)
                grand_total += rows
                print(f"--- Progreso total: {grand_total:,} registros ---\n")
                
        duration = time.time() - overall_start
        avg_rps = grand_total / duration

        print("\n" + "="*50)
        print("🎉 INGESTA MASIVA COMPLETADA")
        print(f"Total Registros: {grand_total:,}")
        print(f"Tiempo Total: {duration/60:.2f} minutos")
        print(f"Velocidad Media: {avg_rps:.0f} rec/s")
        print("="*50)

        # Registro final en ClickHouse
        try:
            self.ch_client.command(f"""
                INSERT INTO analytics.ingestion_benchmarks (test_id, format, records, duration_seconds, avg_rps, timestamp)
                VALUES ('full_ingestion_ultra', 's3_ultra_parallel', {grand_total}, {duration}, {avg_rps}, now())
            """)
        except: pass

if __name__ == "__main__":
    with open('.last_bucket_name', 'r') as f:
        bucket = f.read().strip()
    
    ingestor = S3FullIngestionUltra(bucket)
    ingestor.run()
