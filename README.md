# Real-Time Crypto Market Analytics Platform

An end-to-end **real-time data engineering and analytics platform** that ingests live cryptocurrency trade data, processes it using **Kafka and Spark Structured Streaming**, stores analytics in **AWS S3 (data lake)**, and visualizes insights through a **Streamlit dashboard** querying data directly from S3 using **DuckDB**.

This project demonstrates modern **streaming data architecture**, **cloud-native analytics**, and **production-grade fault-tolerant pipelines**.

---

## Key Features

- Real-time crypto trade ingestion from Coinbase
- Kafka-based streaming pipeline
- Spark Structured Streaming with:
  - Event-time processing
  - Watermarking
  - Windowed aggregations
  - Fault tolerance via checkpoints
- Analytics stored as partitioned Parquet files in AWS S3
- DuckDB querying Parquet directly from S3 (no warehouse needed)
- Interactive Streamlit dashboard with auto-refresh
- Cloud deployment using Streamlit Cloud

---

## 🏗️ System Architecture

```text
Coinbase API
     |
     v
Kafka Producer
     |
     v
Kafka Topic
     |
     v
Spark Structured Streaming
     |
     v
AWS S3 (Parquet Data Lake)
     |
     v
DuckDB
     |
     v
Streamlit Dashboard
```

## Tech Stack
Streaming & Processing

* Apache Kafka
* Apache Spark (Structured Streaming)

#### Storage & Analytics
* AWS S3 (Data Lake)
* Parquet
* DuckDB

#### Dashboard
* Streamlit
* Plotly
