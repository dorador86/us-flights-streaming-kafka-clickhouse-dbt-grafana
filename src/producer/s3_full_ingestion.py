import pandas as pd
import pyarrow.dataset as ds
import s3fs
import time
import queue
import threading
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import clickhouse_connect

# ==========================================
# CONFIGURACIÓN STREAMING PURO (K.I.S.S.)
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

class S3StreamingIngestion:
    def __init__(self):
        self.s3 = s3fs.S3FileSystem(anon=False)
        self.data_queue = queue.Queue(maxsize=10) # Cola de 10 bloques (buffer)
        self.stop_event = threading.Event()
        
        # Configuración Kafka (La de oro)
        self.producer = Producer({
            'bootstrap.servers': BOOTSTRAP_SERVERS,
            'linger.ms': 100, 
            'batch.size': 1048576, # 1MB
            'compression.type': 'lz4',
            'acks': '1',
            'queue.buffering.max.messages': 500000,
        })
        
        registry_client = SchemaRegistryClient({'url': REGISTRY_URL})
        self.serializer = AvroSerializer(registry_client, SCHEMA_STR, lambda obj, ctx: obj)
        self.ctx = SerializationContext(TOPIC, MessageField.VALUE)

    def reader_thread(self, files):
        """Hilo dedicado solo a leer de S3 y alimentar la cola"""
        print("📖 [Reader] Iniciando lectura en background...")
        
        for s3_path in files:
            print(f"📖 [Reader] Abriendo: {s3_path.split('/')[-1]} ...")
            try:
                dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
                cols = ['FlightDate', 'Airline', 'Tail_Number', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'ArrDelay', 'Distance',
                        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                        'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                        'DestCityName', 'DestState', 'AirTime', 'Diverted']
                
                # Leemos en trozos de 100k para fluidez
                for batch in dataset.to_batches(columns=cols, batch_size=100000):
                    if self.stop_event.is_set(): break
                    
                    df = batch.to_pandas()
                    # Transformaciones ligeras
                    df['FlightDate'] = pd.to_datetime(df['FlightDate']).values.astype('datetime64[s]').astype('int64')
                    df['Cancelled'] = df['Cancelled'].astype(int)
                    df['Diverted'] = df['Diverted'].astype(int)
                    df = df.where(pd.notnull(df), None)
                    
                    records = df.to_dict('records')
                    self.data_queue.put(records) # Bloquea si la cola está llena (backpressure natural)
                    
            except Exception as e:
                print(f"⚠️ [Reader] Error leyendo archivo: {e}")
        
        # Señal de fin
        self.data_queue.put(None)
        print("📖 [Reader] Lectura finalizada.")

    def run_ingestion(self):
        # 1. Detectar Archivos
        with open('.last_bucket_name', 'r') as f: bucket = f.read().strip()
        files = [
            f"{bucket}/raw/flights/year=2018/Combined_Flights_2018.parquet",
            f"{bucket}/raw/flights/year=2019/Combined_Flights_2019.parquet",
            f"{bucket}/raw/flights/year=2020/Combined_Flights_2020.parquet",
            f"{bucket}/raw/flights/year=2021/Combined_Flights_2021.parquet",
            f"{bucket}/raw/flights/year=2022/Combined_Flights_2022.parquet"
        ]

        # 2. Arrancar Hilo Lector
        t = threading.Thread(target=self.reader_thread, args=(files,))
        t.start()
        
        # 3. Main Loop (Productor)
        print("🚀 [Main] Iniciando Productor Kafka Continuo...")
        total_sent = 0
        start_time = time.time()
        
        produce = self.producer.produce
        poll = self.producer.poll
        
        try:
            while True:
                records = self.data_queue.get()
                if records is None: break # Fin de datos
                
                for i, record in enumerate(records):
                    try:
                        produce(topic=TOPIC, value=self.serializer(record, self.ctx))
                    except BufferError:
                        poll(0.1)
                        produce(topic=TOPIC, value=self.serializer(record, self.ctx))
                    
                    # Poll ligero para callbacks
                    if i % 10000 == 0:
                        poll(0)
                        
                total_sent += len(records)
                if total_sent % 100000 == 0:
                     print(f"📈 [Main] Enviados: {total_sent:,} (Cola: {self.data_queue.qsize()})")

        except KeyboardInterrupt:
            print("\n🛑 Deteniendo...")
            self.stop_event.set()
            
        t.join()
        self.producer.flush()
        
        duration = time.time() - start_time
        print(f"\n🎉 INGESTA FINALIZADA: {total_sent:,} registros en {duration:.1f}s")

if __name__ == "__main__":
    S3StreamingIngestion().run_ingestion()
