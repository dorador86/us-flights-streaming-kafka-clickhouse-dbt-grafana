from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import time
import os
import json

class S3FlightProducer:
    def __init__(self, bootstrap_servers='localhost:29092', registry_url='http://localhost:8081'):
        schema_registry_conf = {'url': registry_url}
        self.registry_client = SchemaRegistryClient(schema_registry_conf)

        # Esquema Avro (debe coincidir con flights.avsc)
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

        self.serializer = AvroSerializer(self.registry_client, self.schema_str, lambda obj, ctx: obj)
        self.producer = Producer({
            'bootstrap.servers': bootstrap_servers,
            'linger.ms': 50,
            'batch.size': 262144,
            'compression.type': 'lz4',
            'acks': '1',
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 1048576, # 1GB
            'queue.buffering.max.ms': 50
        })
        self.topic = 'flights_avro_pro'
        self.s3 = s3fs.S3FileSystem(anon=False) # True si es público, False si usa credenciales AWS

    def process_and_send(self, df_orig, limit=None):
        count = 0
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        
        # Trabajamos sobre una copia para evitar SettingWithCopyWarning
        df = df_orig.copy()
        
        # Conversiones consistentes
        df['FlightDate'] = pd.to_datetime(df['FlightDate']).view('int64') // 10**6 # Micros -> Millis
        df['Cancelled'] = df['Cancelled'].astype(int)
        df['Diverted'] = df['Diverted'].astype(int)
        df = df.where(pd.notnull(df), None)
        
        records = df.to_dict('records')
        for record in records:
            while True:
                try:
                    val_bytes = self.serializer(record, ctx)
                    self.producer.produce(topic=self.topic, value=val_bytes)
                    break
                except BufferError:
                    self.producer.poll(0.1)
            count += 1
            if limit and count >= limit: break
            if count % 5000 == 0:
                self.producer.poll(0)
        
        self.producer.flush()
        return count

    def benchmark_s3(self, s3_path, format='parquet', limit=100000):
        print(f"\n>>> Starting Benchmark: {s3_path} ({format.upper()})")
        start_time = time.time()
        
        try:
            if format == 'parquet':
                # Lectura eficiente de S3 Parquet usando pyarrow
                import pyarrow.dataset as ds
                dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
                # Solo tomamos las columnas necesarias y el limite
                cols = ['FlightDate', 'Airline', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'Distance',
                        'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                        'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                        'DestCityName', 'DestState', 'AirTime', 'Diverted']
                
                # Leemos en batches hasta el limite
                count = 0
                for batch in dataset.to_batches(columns=cols):
                    df_batch = batch.to_pandas()
                    remaining = limit - count
                    if remaining <= 0: break
                    
                    df_to_send = df_batch.iloc[:min(len(df_batch), remaining)]
                    count += self.process_and_send(df_to_send)
                    if count >= limit: break
                    
            elif format == 'csv':
                # Lectura de S3 CSV usando pandas
                with self.s3.open(s3_path, 'rb') as f:
                    # Leemos en chunks para no saturar memoria
                    chunks = pd.read_csv(f, chunksize=10000, nrows=limit)
                    count = 0
                    for chunk in chunks:
                        count += self.process_and_send(chunk)
                        if count >= limit: break
            
            duration = time.time() - start_time
            avg_rps = count / duration
            print(f"DONE: {count} records in {duration:.2f}s ({avg_rps:.0f} rec/s)")
            
            # Log to ClickHouse
            try:
                import clickhouse_connect
                client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')
                client.command(f"""
                    INSERT INTO analytics.ingestion_benchmarks (test_id, format, records, duration_seconds, avg_rps, timestamp)
                    VALUES ('test_{int(time.time())}', '{format}', {count}, {duration}, {avg_rps}, now())
                """)
                print("✓ Metrics logged to ClickHouse.")
            except Exception as log_e:
                print(f"! Could not log metrics to ClickHouse: {log_e}")
                
            return duration
            
        except Exception as e:
            print(f"FAILED: {str(e)}")
            return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--format", choices=['csv', 'parquet'], default='parquet')
    parser.add_argument("--limit", type=int, default=100000)
    args = parser.parse_args()

    # Rutas detectadas del bucket
    BUCKET = "us-flights-streaming-kafka-clickhouse-dbt-grafana"
    BASE_PATH = f"s3://{BUCKET}/raw/flights/year=2022"
    
    file_name = "Combined_Flights_2022.parquet" if args.format == 'parquet' else "Combined_Flights_2022.csv"
    full_path = f"{BASE_PATH}/{file_name}"
    
    producer = S3FlightProducer()
    producer.benchmark_s3(full_path, format=args.format, limit=args.limit)
