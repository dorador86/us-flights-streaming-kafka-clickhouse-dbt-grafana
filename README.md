# 🛫 US Flights Real-Time Analytics Stack

## 1. Executive Summary
An **End-to-End** data engineering system designed for the real-time ingestion, validation, and analytical processing of over **25GB** of historical US civil aviation data (29M+ records).

The project stands out for its **high efficiency**, processing massive data streams on a resource-constrained infrastructure (**AWS EC2 4GB RAM**), using a **modern ELT architecture** and a state-of-the-art streaming stack.

---

## 2. Technology Stack (Modern Streaming Stack)
*   **Infrastructure:** AWS EC2 (c7i-flex.large), EBS (Storage), S3 (Data Lake).
*   **Containers:** Docker & Docker Compose.
*   **Event Bus:** Apache Kafka (**KRaft** mode for RAM optimization).
*   **Serialization:** Apache **Avro** (maximum payload efficiency).
*   **Analytical Database (OLAP):** **ClickHouse** (Native Kafka ingestion with S3 Storage Policy).
*   **Data Modeling:** **dbt** (dbt-clickhouse) for layered SQL transformations.
*   **Observability:** **Grafana + Prometheus** (System, ingestion, and business metrics).
*   **Language:** Python (Async producers using `multiprocessing` for high throughput).

---

## 3. System Architecture
The system follows a decoupled and reactive data flow:
1.  **Ingestion (S3 to Kafka):** Optimized Python producers read massive Parquet files and emit low-overhead Avro messages.
2.  **Streaming (Kafka):** High-speed buffer without Zookeeper, configured with aggressive retention to minimize disk footprint.
3.  **OLAP (ClickHouse):** The engine consumes directly from topics via `Kafka` Engine tables.
4.  **DLQ (Dead Letter Queue):** Native ClickHouse implementation that diverts records with schema errors or corruption to an audit table without blocking the pipeline.

---

## 4. Technical Challenges & Solutions
### 🚀 High Throughput Ingestion (>80,000 rec/s)
A **Steady-Flow** pattern was implemented in the producers, using `multiprocessing` to parallelize data emit and aggressive `batching`. This allows saturating network bandwidth without exceeding the instance's CPU limits.

### 🧠 Memory Optimization (Hard Limit: 4GB RAM)
Critical configuration of:
*   **JVM Heap:** Tuned for Kafka and Schema Registry to leave headroom for the OS.
*   **ClickHouse Memory Limits:** Use of `materialized_views` to reduce the computational cost of aggregations.
*   **S3 Storage Policy:** ClickHouse writes "cold" data directly to S3, keeping only indexes in RAM/SSD.

---

## 5. Data Layers (Medallion Architecture)
Implemented with **dbt-clickhouse**:

*   **Bronze (Raw):** Raw ingestion with technical type validation.
*   **Silver (Enriched):** Calculation of flight KPIs (speed, time recovery) and delay severity categorization.
*   **Gold (Analytics):** 
    *   *Delay Propagation:* Impact analysis chained by aircraft (`TailNumber`).
    *   *Operational Metrics:* Top 10 airports and airlines by punctuality.

---

## 6. Dashboarding
*   **Ops View:** Total technical control (RAM, CPU, Ingestion Rate, Kafka Lag).
*   **Business View:** Executive summary of the US aviation network performance.

---

## 7. Quick Deployment
```bash
# 1. Spin up the Stack
docker-compose up -d

# 2. Database & Tables Setup
cat src/database/setup_ingestion.sql | docker exec -i clickhouse clickhouse-client

# 3. Inject Data (Simulation)
python3 src/producer/s3_full_ingestion.py

# 4. dbt Transformations
cd dbt_flights && dbt run
```