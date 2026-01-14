-- SLIM CONFIGURATION: 11 Essential Columns + DLQ
CREATE DATABASE IF NOT EXISTS flights;
CREATE DATABASE IF NOT EXISTS analytics;

-- Cleanup
DROP TABLE IF EXISTS flights.flights_mv;
DROP VIEW IF EXISTS flights.flights_errors_mv;
DROP TABLE IF EXISTS flights.flights_errors;
DROP TABLE IF EXISTS flights.flights_queue;
DROP TABLE IF EXISTS flights.flights_raw;
DROP TABLE IF EXISTS analytics.ingestion_benchmarks;

-- 1. Final Flights Table (OLAP) - ONLY 11 COLUMNS
CREATE TABLE flights.flights_raw (
    FlightDate Date32,
    Airline String,
    Tail_Number Nullable(String),
    Origin String,
    Dest String,
    Cancelled Bool,
    Diverted Bool,
    DepDelay Nullable(Float32),
    ArrDelay Nullable(Float32),
    AirTime Nullable(Float32),
    Distance Float32
) ENGINE = MergeTree 
ORDER BY (FlightDate, Airline, Origin)
SETTINGS storage_policy = 's3_main';

-- 2. DEAD LETTER QUEUE Table
CREATE TABLE flights.flights_errors (
    topic String,
    partition Int64,
    offset Int64,
    raw_message String,
    error_message String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY timestamp;

-- 3. Kafka Table (Ingestion Queue)
CREATE TABLE flights.flights_queue (
    FlightDate Int64,
    Airline String,
    Tail_Number Nullable(String),
    Origin String,
    Dest String,
    Cancelled Int32,
    Diverted Int32,
    DepDelay Nullable(Float32),
    ArrDelay Nullable(Float32),
    AirTime Nullable(Float32),
    Distance Float32
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'flights_avro_pro',
    kafka_group_name = 'final_ingestion_group',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_handle_error_mode = 'stream';

-- 4. Materialized View for ERRORS (DLQ)
CREATE MATERIALIZED VIEW flights.flights_errors_mv TO flights.flights_errors AS
SELECT
    _topic as topic,
    _partition as partition,
    _offset as offset,
    _raw_message as raw_message,
    _error as error_message
FROM flights.flights_queue
WHERE _error != '';

-- 5. Materialized View for VALID DATA
CREATE MATERIALIZED VIEW flights.flights_mv TO flights.flights_raw AS
SELECT
    toDate32(fromUnixTimestamp(FlightDate)) AS FlightDate,
    Airline, Tail_Number, Origin, Dest,
    toBool(Cancelled) AS Cancelled,
    toBool(Diverted) AS Diverted,
    DepDelay, ArrDelay, AirTime, Distance
FROM flights.flights_queue
WHERE _error = '';

-- 6. Benchmarks Table
CREATE TABLE analytics.ingestion_benchmarks (
    test_id String,
    format String,
    records UInt64,
    duration_seconds Float64,
    avg_rps Float64,
    timestamp DateTime
) ENGINE = MergeTree
ORDER BY timestamp;
