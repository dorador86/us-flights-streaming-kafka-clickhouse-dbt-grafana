import os
import time
import pandas as pd
import pyarrow.dataset as ds
import s3fs
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
import clickhouse_connect

class S3FullIngestion:
    def __init__(self, bucket_name, topic="flights_avro_pro"):
        self.bucket_name = bucket_name
        self.topic = topic
        
        # S3 Filesystem
        self.s3 = s3fs.S3FileSystem(anon=False)
        
        # Kafka & Schema Registry Config (External Ports)
        sr_config = {'url': 'http://localhost:8081'}
        self.schema_registry_client = SchemaRegistryClient(sr_config)
        
        # Consistent Avro Schema
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
        
        self.serializer = AvroSerializer(
            self.schema_registry_client,
            self.schema_str,
            lambda obj, ctx: obj
        )
        
        self.producer_config = {
            'bootstrap.servers': 'localhost:29092',
            'linger.ms': 50,
            'batch.size': 262144,
            'compression.type': 'lz4',
            'acks': '1',
            'queue.buffering.max.messages': 1000000,
            'queue.buffering.max.kbytes': 1048576,
            'queue.buffering.max.ms': 50
        }
        self.producer = Producer(self.producer_config)
        self.ctx = SerializationContext(self.topic, MessageField.VALUE)
        
        # ClickHouse for logging
        self.ch_client = clickhouse_connect.get_client(host='localhost', username='admin', password='admin')

    def process_file(self, s3_path):
        print(f"-> Processing: {s3_path}")
        start_time = time.time()
        
        dataset = ds.dataset(s3_path, filesystem=self.s3, format="parquet")
        
        cols = ['FlightDate', 'Airline', 'Origin', 'Dest', 'Cancelled', 'DepDelay', 'Distance',
                'Year', 'Quarter', 'Month', 'DayofMonth', 'DayOfWeek', 
                'Marketing_Airline_Network', 'OriginCityName', 'OriginState', 
                'DestCityName', 'DestState', 'AirTime', 'Diverted']
        
        total_rows = 0
        for batch in dataset.to_batches(columns=cols):
            df = batch.to_pandas().copy()
            
            # Conversions
            df['FlightDate'] = pd.to_datetime(df['FlightDate']).view('int64') // 10**6
            df['Cancelled'] = df['Cancelled'].astype(int)
            df['Diverted'] = df['Diverted'].astype(int)
            df = df.where(pd.notnull(df), None)
            
            records = df.to_dict('records')
            for record in records:
                while True:
                    try:
                        val_bytes = self.serializer(record, self.ctx)
                        self.producer.produce(topic=self.topic, value=val_bytes)
                        break
                    except BufferError:
                        self.producer.poll(0.1)
                
                total_rows += 1
                if total_rows % 10000 == 0:
                    self.producer.poll(0)
            
        self.producer.flush()
        duration = time.time() - start_time
        print(f"✓ {s3_path} done: {total_rows} records in {duration:.2f}s ({total_rows/duration:.0f} rec/s)")
        return total_rows, duration

    def run(self):
        print(f"🚀 Starting Mass Ingestion from S3 Bucket: {self.bucket_name}")
        
        search_path = f"{self.bucket_name}/raw/flights/"
        all_files = sorted(self.s3.glob(f"{search_path}**/*.parquet"))
        
        if not all_files:
            print("! No Parquet files found in the bucket structure.")
            return

        print(f"Found {len(all_files)} files to ingest.")
        
        grand_total_rows = 0
        overall_start = time.time()
        
        for file_path in all_files:
            rows, _ = self.process_file(file_path)
            grand_total_rows += rows
            print(f"--- Progress: {grand_total_rows:,} records processed ---\n")
            
        overall_duration = time.time() - overall_start
        avg_rps = grand_total_rows / overall_duration

        print("\n" + "="*50)
        print("🎉 MASS INGESTION COMPLETE")
        print(f"Total Records: {grand_total_rows:,}")
        print(f"Total Time: {overall_duration/60:.2f} minutes")
        print(f"Avg Throughput: {avg_rps:.0f} rec/s")
        print("="*50)

        # Log metrics to ClickHouse
        try:
            self.ch_client.insert('analytics.ingestion_benchmarks', [
                ['full_run', 'PARQUET_S3', grand_total_rows, overall_duration, avg_rps, pd.Timestamp.now()]
            ], column_names=['test_id', 'format', 'records', 'duration_seconds', 'avg_rps', 'timestamp'])
            print("✓ Full Ingestion Metrics logged to ClickHouse.")
        except Exception as e:
            print(f"! Failed to log metrics: {e}")

if __name__ == "__main__":
    with open('.last_bucket_name', 'r') as f:
        bucket = f.read().strip()
    
    ingestor = S3FullIngestion(bucket)
    ingestor.run()
