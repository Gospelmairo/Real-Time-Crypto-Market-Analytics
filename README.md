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
#### Streaming & Processing
* Apache Kafka
* Apache Spark (Structured Streaming)

#### Storage & Analytics
* AWS S3 (Data Lake)
* Parquet
* DuckDB

#### Dashboard
* Streamlit
* Plotly

#### Infrastructure
* Docker & Docker Compose
* AWS
* Python 3.11+

## 📁 Project Structure
Real-Time-Crypto-Market-Analytics/
│
├── dashboard/                     # Streamlit dashboard
│   ├── .streamlit/
│   │   └── secrets.toml           # AWS credentials (Streamlit Cloud)
│   ├── app.py                     # Main dashboard app
│   └── test.py
│
├── ingestion/                     # Kafka producer
│   ├── coinbase_producer.py
│   └── config.py
│
├── kafka/                         # Kafka configuration
│
├── spark/                         # Spark streaming jobs
│   ├── analytics_trades.py
│   ├── clean_trades.py
│   └── sql/
│       └── market_sql_analytics.py
│
├── storage/
│   ├── analytics/                 # S3 analytics output
│   └── checkpoints/               # Spark checkpoints
│
├── docker/
│   └── docker-compose.yml
│
├── .gitignore
├── README.md
└── requirements.txt



## 🔄 Data Flow Explanation
**1. Ingestion**
* Live crypto trade data is pulled from Coinbase
* Data is produced to Kafka topics


**2. Stream Processing**
* Spark Structured Streaming consumes Kafka data
* Applies event-time windows and aggregations
* Handles late data using watermarks
* Writes aggregated analytics to S3 as Parquet


**3. Storage**
* Data stored in AWS S3
* Partitioned by symbol
* Optimized for analytics queries


**4. Analytics & Visualization**
* DuckDB queries Parquet files directly from S3
* Streamlit dashboard renders metrics and charts
* Auto-refresh enabled for near real-time updates


## 🧪 Local Development Setup
1️⃣ Clone Repository
```bash
git clone https://github.com/your-username/Real-Time-Crypto-Market-Analytics.git
cd Real-Time-Crypto-Market-Analytics
```

2️⃣ Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3️⃣ Start Kafka & Infrastructure
```bash
docker-compose up -d
```
