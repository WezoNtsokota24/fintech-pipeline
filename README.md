
# Real-Time Fintech Market Data Pipeline 📈

A high-performance, containerized data engineering pipeline designed to ingest, process, and store live cryptocurrency market data (BTC-USD) using a **Medallion Architecture**.



## 🚀 Overview
This project demonstrates an end-to-end streaming architecture. It handles the "unbounded" nature of financial markets by using a message broker to buffer high-velocity ticks and Spark to enforce structure before saving to a Data Lake.

### Tech Stack
* **Ingestion:** Python WebSockets (Coinbase Pro API)
* **Message Broker:** Redpanda (Kafka-compatible, low-latency)
* **Processing:** PySpark Streaming
* **Storage:** MinIO (S3-compatible Object Storage)
* **Data Format:** Apache Parquet (Columnar storage)
* **Orchestration:** Docker & Docker Compose

## 🏗️ Architecture
1.  **Producer:** A Python service connects to Coinbase WebSockets and streams live trades into a Redpanda topic (`market-topic`).
2.  **Streaming Layer:** Redpanda acts as a resilient buffer to handle backpressure during high market volatility.
3.  **Processor:** A PySpark job consumes the stream, enforces a strict schema, and performs data type conversions.
4.  **Data Lake:** Processed data is written as partitioned Parquet files into a MinIO bucket for downstream analytical use.



## 🛠️ Setup & Usage

### Prerequisites
* Docker & Docker Compose
* Python 3.9+

### 1. Spin up the Infrastructure
```bash
docker-compose up -d

```

### 2. Prepare the Data Lake

* Navigate to `http://localhost:9001` (MinIO UI).
* Login with `admin` / `password`.
* Create a bucket named `market-data`.

### 3. Start the Ingestion

```bash
# Install dependencies
pip install confluent-kafka websockets

# Start streaming data
python producer.py

```

### 4. Monitor the Processing

View the Spark logs to see real-time batch processing:

```bash
docker logs -f fintech-pipeline-main-spark-processor-1

```

## 📊 Data Preview

The final output is stored in **Parquet** format, which allows for efficient "Time Travel" queries and high-performance analytics.

| price | product_id | time |
| --- | --- | --- |
| 96450.20 | BTC-USD | 2026-01-04T15:20:01Z |
| 96451.05 | BTC-USD | 2026-01-04T15:20:05Z |

## 🧠 Key Learning Outcomes

* **Handling Unbounded Data:** Implementing streaming vs. batch logic.
* **Containerization:** Managing multi-service networking within Docker.
* **Schema Enforcement:** Ensuring data quality in a "Schema-on-Read" environment.
* **Storage Optimization:** Utilizing Parquet for cost-effective financial data archiving.


