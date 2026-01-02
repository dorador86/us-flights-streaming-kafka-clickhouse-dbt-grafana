import pandas as pd
import pyarrow.parquet as pq
import time
import json
import os
import io
from confluent_kafka import Producer
import fastavro

class FlightProducer:
    def __init__(self, bootstrap_servers='localhost:9092', schema_path='src/producer/flight_schema.avsc'):
        # Configuración optimizada para rendimiento
        self.producer_conf = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'flights-benchmark-producer',
            'linger.ms': 10,               # Espera 10ms para agrupar mensajes
            'batch.num.messages': 1000,    # Agrupa hasta 1000 mensajes
            'queue.buffering.max.messages': 100000
        }
        self.producer = Producer(self.producer_conf)
        self.schema = self._load_schema(schema_path)
        self.topic = 'raw_flights'

    def _load_schema(self, path):
        with open(path, "r") as f:
            return fastavro.parse_schema(json.load(f))

    def _delivery_report(self, err, msg):
        if err is not None:
            print(f"Error en envío: {err}")

    def produce_parquet(self, file_path, limit=50000):
        """Lee Parquet por bloques (Batches) para ahorrar RAM."""
        print(f"\n>>> Iniciando Ingesta desde PARQUET: {file_path}")
        start_time = time.time()
        count = 0
        
        parquet_file = pq.ParquetFile(file_path)
        
        # Iteramos por batches de 1000 filas
        for batch in parquet_file.iter_batches(batch_size=1000):
            df = batch.to_pandas()
            # Limpieza básica para Avro
            df['FlightDate'] = df['FlightDate'].astype('int64') // 1000
            
            for _, row in df.iterrows():
                self._send_to_kafka(row.to_dict())
                count += 1
                if count >= limit: break
            
            if count >= limit: break
            print(f"  [Parquet] Mensajes enviados: {count}")
            self.producer.poll(0)

        self.producer.flush()
        return count, time.time() - start_time

    def produce_csv(self, file_path, limit=50000):
        """Lee CSV por trozos (Chunks) para ahorrar RAM."""
        print(f"\n>>> Iniciando Ingesta desde CSV: {file_path}")
        start_time = time.time()
        count = 0
        
        # Leemos en trozos de 1000
        for chunk in pd.read_csv(file_path, chunksize=1000):
            # Adaptamos FlightDate (en CSV suele ser String)
            chunk['FlightDate'] = pd.to_datetime(chunk['FlightDate']).astype('int64') // 10**6
            
            for _, row in chunk.iterrows():
                self._send_to_kafka(row.to_dict())
                count += 1
                if count >= limit: break
            
            if count >= limit: break
            print(f"  [CSV] Mensajes enviados: {count}")
            self.producer.poll(0)

        self.producer.flush()
        return count, time.time() - start_time

    def _send_to_kafka(self, record):
        """Serializa en Avro y envía. Limpia NaN de Pandas para que Avro acepte 'None'."""
        # Limpieza de nulos de Pandas (NaN -> None) para compatibilidad con Avro
        cleaned_record = {k: (None if pd.isna(v) else v) for k, v in record.items()}
        
        fo = io.BytesIO()
        fastavro.schemaless_writer(fo, self.schema, cleaned_record)
        self.producer.produce(
            self.topic, 
            value=fo.getvalue(), 
            callback=self._delivery_report
        )

def run_benchmark():
    producer = FlightProducer()
    parquet_path = "data/raw/Combined_Flights_2022.parquet"
    csv_path = "data/raw/flights_sample.csv"
    limit = 20000 # Probamos con 20k para que sea rápido pero significativo
    
    # 1. Benchmark Parquet
    p_count, p_time = producer.produce_parquet(parquet_path, limit=limit)
    
    # 2. Benchmark CSV
    c_count, c_time = producer.produce_csv(csv_path, limit=limit)
    
    print("\n" + "="*40)
    print("      RESULTADOS DEL BENCHMARK")
    print("="*40)
    print(f"PARQUET: {p_count} filas en {p_time:.2f}s ({p_count/p_time:.2f} rec/s)")
    print(f"CSV:     {c_count} filas en {c_time:.2f}s ({c_count/c_time:.2f} rec/s)")
    print("="*40)

if __name__ == "__main__":
    run_benchmark()
