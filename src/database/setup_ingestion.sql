-- 1. Crear la base de datos de vuelos
CREATE DATABASE IF NOT EXISTS flights;

-- 2. Tabla de Destino (Capa Bronce - RAW)
-- Aquí es donde los datos se guardarán de forma permanente
CREATE TABLE IF NOT EXISTS flights.flights_raw (
    FlightDate Date32,
    Year UInt16,
    Month UInt8,
    DayofMonth UInt8,
    DayOfWeek UInt8,
    Airline String,
    Tail_Number Nullable(String),
    Flight_Number_Operating_Airline Int32,
    Origin String,
    OriginCityName String,
    OriginState String,
    Dest String,
    DestCityName String,
    DestState String,
    CRSDepTime Int32,
    DepTime Nullable(Float32),
    DepDelay Nullable(Float32),
    DepDelayMinutes Nullable(Float32),
    CRSArrTime Int32,
    ArrTime Nullable(Float32),
    ArrDelay Nullable(Float32),
    ArrDelayMinutes Nullable(Float32),
    AirTime Nullable(Float32),
    Distance Float32,
    Cancelled Boolean,
    Diverted Boolean,
    TaxiOut Nullable(Float32),
    TaxiIn Nullable(Float32),
    CarrierDelay Nullable(Float32),
    WeatherDelay Nullable(Float32),
    NASDelay Nullable(Float32),
    SecurityDelay Nullable(Float32),
    LateAircraftDelay Nullable(Float32)
) ENGINE = MergeTree()
ORDER BY (FlightDate, Airline, Origin);

-- 3. Tabla de Motor Kafka (La cola de ingesta)
-- Esta tabla NO almacena datos, solo los consume del topic
CREATE TABLE IF NOT EXISTS flights.flights_kafka_queue (
    FlightDate Int64, -- Avro timestamp-micros viene como long
    Year Int32,
    Month Int32,
    DayofMonth Int32,
    DayOfWeek Int32,
    Airline String,
    Tail_Number Nullable(String),
    Flight_Number_Operating_Airline Int32,
    Origin String,
    OriginCityName String,
    OriginState String,
    Dest String,
    DestCityName String,
    DestState String,
    CRSDepTime Int32,
    DepTime Nullable(Float32),
    DepDelay Nullable(Float32),
    DepDelayMinutes Nullable(Float32),
    CRSArrTime Int32,
    ArrTime Nullable(Float32),
    ArrDelay Nullable(Float32),
    ArrDelayMinutes Nullable(Float32),
    AirTime Nullable(Float32),
    Distance Float32,
    Cancelled Boolean,
    Diverted Boolean,
    TaxiOut Nullable(Float32),
    TaxiIn Nullable(Float32),
    CarrierDelay Nullable(Float32),
    WeatherDelay Nullable(Float32),
    NASDelay Nullable(Float32),
    SecurityDelay Nullable(Float32),
    LateAircraftDelay Nullable(Float32)
) ENGINE = Kafka()
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'raw_flights',
    kafka_group_name = 'clickhouse_ingest_group_json',
    kafka_format = 'JSONEachRow',
    kafka_max_block_size = 1000,
    kafka_skip_broken_messages = 1;

-- 4. Vista Materializada (El motor de transferencia)
-- Transforma el timestamp de Avro a Date32 de ClickHouse al vuelo
CREATE MATERIALIZED VIEW IF NOT EXISTS flights.flights_mv TO flights.flights_raw AS
SELECT 
    toDate32(fromUnixTimestamp64Micro(FlightDate)) AS FlightDate,
    toUInt16(Year) AS Year,
    toUInt8(Month) AS Month,
    toUInt8(DayofMonth) AS DayofMonth,
    toUInt8(DayOfWeek) AS DayOfWeek,
    Airline,
    Tail_Number,
    Flight_Number_Operating_Airline,
    Origin,
    OriginCityName,
    OriginState,
    Dest,
    DestCityName,
    DestState,
    CRSDepTime,
    DepTime,
    DepDelay,
    DepDelayMinutes,
    CRSArrTime,
    ArrTime,
    ArrDelay,
    ArrDelayMinutes,
    AirTime,
    Distance,
    Cancelled,
    Diverted,
    TaxiOut,
    TaxiIn,
    CarrierDelay,
    WeatherDelay,
    NASDelay,
    SecurityDelay,
    LateAircraftDelay
FROM flights.flights_kafka_queue;
