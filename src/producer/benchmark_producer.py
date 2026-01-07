from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import pandas as pd
import pyarrow.parquet as pq
import time
import json

class FlightRegistryProducer:
    def __init__(self, bootstrap_servers='localhost:29092', registry_url='http://localhost:8081'):
        # 1. Configurar Cliente del Registry
        schema_registry_conf = {'url': registry_url}
        self.registry_client = SchemaRegistryClient(schema_registry_conf)

        # 2. Definir Esquema Avro (Coincide con ClickHouse)
        self.schema_str = """
        {
          "type": "record",
          "name": "FlightRecord",
          "namespace": "com.flights",
          "fields": [
            {"name": "FlightDate", "type": "long"},
            {"name": "Airline", "type": "string"},
            {"name": "Origin", "type": "string"},
            {"name": "Dest", "type": "string"},
            {"name": "Cancelled", "type": "int"},
            {"name": "DepDelay", "type": ["null", "float"], "default": null},
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

        # 3. Serializador Optimizado
        self.serializer = AvroSerializer(self.registry_client,
                                         self.schema_str,
                                         lambda obj, ctx: obj)

        # 4. Config Kafka (High Throughput)
        self.producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 20, # Esperar un poco para hacer batches más grandes
            'batch.size': 131072, # 128KB batches
            'compression.type': 'lz4', # Compresión rápida
            'acks': '1' # Rapidez sobre durabilidad extrema
        }
        self.producer = Producer(self.producer_conf)
        self.topic = 'flights_avro_pro'

    def produce_from_parquet(self, file_path, limit=None):
        print(f"\n>>> [AVRO-REGISTRY] Ingesting from: {file_path}")
        start_time = time.time()
        
        parquet_file = pq.ParquetFile(file_path)
        count = 0
        total_batches = 0
        
        # Columnas necesarias
        cols = ['FlightDate', 'Airline', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'Distance',
                'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                'DestCityName', 'DestState', 'AirTime', 'Diverted']
        
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        
        try:
            for batch in parquet_file.iter_batches(batch_size=5000, columns=cols):
                df = batch.to_pandas()
                
                # Conversiones Rápidas
                df['FlightDate'] = df['FlightDate'].astype('int64') // 1000 # Micros -> Millis
                df['Cancelled'] = df['Cancelled'].astype(int)
                df['Diverted'] = df['Diverted'].astype(int)
                
                # Manejo eficiente de nulos
                # DepDelay y AirTime son float, si son NaN los pasamos a None
                df = df.where(pd.notnull(df), None)
                
                records = df.to_dict('records')
                
                for record in records:
                    # Serializar y Enviar
                    # La primera vez registra el esquema, luego usa cache (muy rápido)
                    val_bytes = self.serializer(record, ctx)
                    self.producer.produce(topic=self.topic, value=val_bytes)
                    
                    count += 1
                    if limit and count >= limit: break
                
                # Poll asíncrono para liberar buffer
                self.producer.poll(0)
                
                total_batches += 1
                if total_batches % 10 == 0:
                     elapsed = time.time() - start_time
                     print(f"  Sent {count} rows... Rate: {count/elapsed:.0f} rec/s")

                if limit and count >= limit: break

        except KeyboardInterrupt:
            print("\nStopping ingestion...")
        
        print("\nFlushing producer...")
        self.producer.flush()
        
        duration = time.time() - start_time
        print(f"DONE: {count} rows in {duration:.2f}s ({count/duration:.0f} rec/s)")

if __name__ == "__main__":
    producer = FlightRegistryProducer()
    # Ingestar 500,000 registros para una buena prueba de carga
    producer.produce_from_parquet("data/raw/Combined_Flights_2022.parquet", limit=500000)
