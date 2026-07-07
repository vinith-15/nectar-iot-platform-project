# Nectar IoT Data Platform

A complete, end‑to‑end data engineering platform for ingesting, processing, storing, and serving IoT telemetry data in real time.

---

## 📖 Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Key Components](#key-components)
- [API Endpoints](#api-endpoints)
- [Dashboard](#dashboard)
- [Data Flow](#data-flow)
- [SQL Queries](#sql-queries)
- [Data Quality Framework](#data-quality-framework)
- [Monitoring](#monitoring)
- [Deliverables](#deliverables)
- [License](#license)

---

## 📌 Overview

This project implements a scalable IoT data pipeline that:
- Ingests high‑volume telemetry from simulated IoT devices via **Kafka**.
- Processes streaming data with **Apache Spark** (Structured Streaming) and stores it in **Delta Lake** (Bronze/Silver/Gold layers).
- Performs batch aggregations every 2 minutes orchestrated by **Airflow**.
- Loads aggregated results into **PostgreSQL** (star schema).
- Serves data via **FastAPI** REST endpoints and a **Streamlit** dashboard.
- Models asset hierarchy using **Neo4j** graph database.
- Monitors system health with **Prometheus** and **Grafana**.

---

## 🏗️ Architecture
<img width="607" height="891" alt="image" src="https://github.com/user-attachments/assets/a0e90bf8-9a6d-4df5-9b91-830a18530cce" />
🧰 Technology Stack
Layer	Technology
Ingestion	Kafka (with Zookeeper)
Processing	Apache Spark 3.5.0 (Structured Streaming)
Storage	Delta Lake (on local file system)
Analytics DB	PostgreSQL 16
Graph DB	Neo4j 5‑community
Orchestration	Apache Airflow 2.8.0
API	FastAPI
Dashboard	Streamlit
Monitoring	Prometheus, Grafana
Containerization	Docker & Docker Compose
Data Quality	Great Expectations
Language	Python 3.10+
⚙️ Prerequisites
Docker and Docker Compose (version 2.24+)

Python 3.10+ (for local scripts)

Git (optional)

🚀 Quick Start
1. Clone or create the project
bash
git clone https://github.com/vinith-15/nectar-iot-platform-project.git
cd nectar-iot-platform
2. Start all services (Docker)
bash
docker-compose up -d
Wait 30–40 seconds for services to initialize.

3. Create Kafka topics
bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic iot-telemetry --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic iot-events --partitions 3 --replication-factor 1
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --create --topic asset-metadata --partitions 3 --replication-factor 1
4. Apply PostgreSQL schema
bash
docker exec -i postgres psql -U admin -d postgres < sql/init.sql
5. Start Spark Streaming jobs
bash
docker exec -d spark-master /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/spark/data/streaming/bronze_streaming.py
docker exec -d spark-master /opt/spark/bin/spark-submit --packages io.delta:delta-core_2.12:3.0.0 /opt/spark/data/streaming/silver_streaming.py
6. Run the IoT simulator
bash
python3 simulator/generator.py &
7. Import Neo4j hierarchy (optional)
bash
python3 neo4j/import.py
8. Unpause Airflow DAG
Open http://localhost:8081

Login with admin / admin

Toggle iot_pipeline_dag to ON
<img width="840" height="598" alt="image" src="https://github.com/user-attachments/assets/88472039-fde9-4570-9ea6-433222ea309b" />
 🔄 Data Flow Summary
IoT Simulator → Kafka (topics: iot-telemetry, iot-events, asset-metadata)

Spark Bronze → Delta Bronze (raw, validated)

Spark Silver → Delta Silver (cleaned, with timestamps)

Airflow (every 2 min) triggers:

Spark Gold → Delta Gold (aggregated metrics)

Spark Loader → PostgreSQL (fact tables)

FastAPI and Streamlit query PostgreSQL to serve the data.
