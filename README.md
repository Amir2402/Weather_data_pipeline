# 🌦️ Weather Data Pipeline  

A modern data pipeline built with **Apache Airflow**, **duckdb** and **S3(Minio)** to fetch, process, and analyze weather data using the **Medallion Architecture** (Bronze → Silver → Gold).  

This project demonstrates **best practices in data engineering**, including orchestration, incremental data processing, and building reproducible pipelines.  

---

## 🚀 Features  

- 📥 **Ingestion Layer (Bronze)**: Fetches daily/hourly weather data from Open-Meteo API and stores it in object storage (S3/MinIO).  
- 🔄 **Transformation Layer (Silver)**: Cleans and normalizes raw JSON data into structured tables.  
- 📊 **Analytics Layer (Gold)**: Aggregates and enriches weather data (e.g., temperature trends, rankings).  
- ⚙️ **Orchestration with Airflow**: DAGs automate ingestion, transformation, and aggregation.  
- 🛠️ **DuckDB** for local querying and transformations.  
- 📦 **Dockerized Environment** for easy deployment (Astronomer-compatible).  

---

## 🏗️ Architecture  
      ┌───────────┐
      │ OpenMeteo │
      └─────┬─────┘
            │
      ┌─────▼─────┐
      │   Bronze   │  → Raw JSON in S3/MinIO
      └─────┬─────┘
            │
      ┌─────▼─────┐
      │   Silver   │  → Cleaned structured tables (DuckDB)
      └─────┬─────┘
            │
      ┌─────▼─────┐
      │    Gold    │  → Aggregated analytics tables
      └───────────┘


---

## ⚙️ Tech Stack  

- **Python** (Programming Language)  
- **Apache Airflow** (Orchestration)  
- **DuckDB** (SQL Processing Engine)  
- **S3 / MinIO** (Object Storage)  
- **Docker + Astro CLI** (deployment)  