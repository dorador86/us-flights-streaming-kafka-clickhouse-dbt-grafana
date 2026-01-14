# 🛫 US Flights Real-Time Analytics Stack

## 1. Resumen Ejecutivo (El "Elevator Pitch")
Sistema de ingeniería de datos **End-to-End** diseñado para la ingesta, validación y análisis analítico en tiempo real de más de **25GB** de datos históricos de aviación civil de EE.UU. (29M+ registros). 

El proyecto destaca por su **alta eficiencia**, procesando flujos masivos de datos en una infraestructura optimizada de recursos limitados (**AWS EC2 4GB RAM**), utilizando una arquitectura **ELT moderna** y un stack de streaming de última generación.

---

## 2. Stack Tecnológico (Modern Streaming Stack)
*   **Infraestructura:** AWS EC2 (c7i-flex.large), EBS (Storage), S3 (Data Lake).
*   **Contenedores:** Docker & Docker Compose.
*   **Bus de Eventos:** Apache Kafka (modo **KRaft** para ahorro de RAM).
*   **Serialización:** Apache **Avro** (máxima eficiencia de payload).
*   **Base de Datos Analítica (OLAP):** **ClickHouse** (Ingesta nativa de Kafka con S3 Storage Policy).
*   **Modelado de Datos:** **dbt** (dbt-clickhouse) para transformaciones SQL en capas.
*   **Observabilidad:** **Grafana + Prometheus** (Métricas de sistema, ingesta y negocio).
*   **Lenguaje:** Python (Productores asíncronos con `aiokafka` y `multiprocessing`).

---

## 3. Arquitectura del Sistema
El sistema sigue un flujo de datos desacoplado y reactivo:
1.  **Ingesta (S3 to Kafka):** Productores Python optimizados leen archivos Parquet masivos y emiten mensajes Avro de bajo peso.
2.  **Streaming (Kafka):** Buffer de alta velocidad sin Zookeeper, configurado con retención agresiva para minimizar el footprint de disco.
3.  **OLAP (ClickHouse):** El motor consume directamente de los tópicos mediante tablas `Kafka` Engine.
4.  **DLQ (Dead Letter Queue):** Implementación nativa en ClickHouse que desvía registros con errores de esquema o corrupción a una tabla de auditoría sin bloquear el pipeline.

---

## 4. Desafíos Técnicos y Soluciones
### 🚀 Ingesta de Alto Throughput (>80,000 rec/s)
Se implementó un patrón de **Steady-Flow** en los productores, utilizando `multiprocessing` para paralelizar el envío y `batching` agresivo. Esto permite saturar el ancho de banda sin superar los límites de CPU de la instancia.

### 🧠 Optimización de Memoria (Hard Limit: 4GB RAM)
Configuración crítica de:
*   **JVM Heap:** Ajustada para Kafka y Schema Registry para dejar aire al sistema.
*   **ClickHouse Memory Limits:** Uso de `materialized_views` para reducir el coste computacional de las agregaciones.
*   **S3 Storage Policy:** ClickHouse escribe los datos "fríos" directamente a S3, manteniendo solo los índices en RAM/SSD.

---

## 5. Capas de Datos (Arquitectura Medallón)
Implementado con **dbt-clickhouse**:

*   **Bronze (Raw):** Ingesta cruda con validación de tipo técnica.
*   **Silver (Enriched):** Cálculo de KPIs de vuelo (velocidad, recuperación de tiempo) y categorización de severidad del retraso.
*   **Gold (Analytics):** 
    *   *Propagación de Retrasos:* Análisis de impacto encadenado por aeronave (`TailNumber`).
    *   *Métricas Operativas:* Top 10 aeropuertos y aerolíneas por puntualidad.

---

## 6. Dashboarding
*   **Ops View:** Control técnico total (RAM, CPU, Ingestion Rate, Kafka Lag).
*   **Business View:** Resumen ejecutivo de la red de aviación de EE.UU.

---

## 7. Despliegue Rápido
```bash
# 1. Levantar Stack
docker-compose up -d

# 2. Setup DB & Tables
cat src/database/setup_ingestion.sql | docker exec -i clickhouse clickhouse-client

# 3. Inyectar Datos (Simulación)
python3 src/producer/s3_full_ingestion.py

# 4. Transformaciones dbt
cd dbt_flights && dbt run
```