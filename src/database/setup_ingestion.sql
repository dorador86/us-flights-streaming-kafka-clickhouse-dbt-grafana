-- CONFIGURACIÓN FINAL: Vuelos con Avro Confluent + Schema Registry
CREATE DATABASE IF NOT EXISTS flights;

-- Limpiamos todo para asegurar una prueba limpia de rendimiento
DROP TABLE IF EXISTS flights.flights_mv;
DROP TABLE IF EXISTS flights.flights_queue;
-- Opcional: TRUNCATE TABLE flights.flights_raw; -- Si quieres empezar de cero en Grafana
TRUNCATE TABLE flights.flights_raw;

-- 1. Tabla Final (OLAP) - Optimizada para consultas
CREATE TABLE IF NOT EXISTS flights.flights_raw (
    FlightDate Date32,
    Airline String,
    Origin String,
    Dest String,
    Cancelled Bool,
    DepDelay Nullable(Float32),
    Distance Float32,
    Year Int32,
    Quarter Int32,
    Month Int32,
    DayofMonth Int32,
    DayOfWeek Int32,
    Marketing_Airline_Network String,
    OriginCityName String,
    OriginState String,
    DestCityName String,
    DestState String,
    AirTime Nullable(Float32),
    Diverted Bool
) ENGINE = MergeTree 
ORDER BY (FlightDate, Airline, Origin);

-- 2. Tabla Kafka (Cola de Ingesta)
CREATE TABLE flights.flights_queue (
    FlightDate Int64,
    Airline String,
    Origin String,
    Dest String,
    Cancelled Int32, -- Avro int -> Bool en MV
    DepDelay Nullable(Float32),
    Distance Float32,
    Year Int32,
    Quarter Int32,
    Month Int32,
    DayofMonth Int32,
    DayOfWeek Int32,
    Marketing_Airline_Network String,
    OriginCityName String,
    OriginState String,
    DestCityName String,
    DestState String,
    AirTime Nullable(Float32),
    Diverted Int32 -- Avro int -> Bool en MV
) ENGINE = Kafka
SETTINGS 
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'flights_avro_pro',
    kafka_group_name = 'flights_registry_group_v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081';

-- 3. Materialized View (Transformación y Carga)
CREATE MATERIALIZED VIEW flights.flights_mv TO flights.flights_raw AS
SELECT
    toDate32(fromUnixTimestamp64Milli(FlightDate)) AS FlightDate,
    Airline,
    Origin,
    Dest,
    toBool(Cancelled) AS Cancelled,
    DepDelay,
    Distance,
    Year,
    Quarter,
    Month,
    DayofMonth,
    DayOfWeek,
    Marketing_Airline_Network,
    OriginCityName,
    OriginState,
    DestCityName,
    DestState,
    AirTime,
    toBool(Diverted) AS Diverted
FROM flights.flights_queue;
