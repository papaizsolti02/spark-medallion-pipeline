# spark-medallion-pipeline

Distributed PySpark data pipeline running on a Docker-based Spark cluster that processes large-scale NYC Taxi trip data (~100M rows).  
The project implements the **Medallion Architecture (Bronze / Silver / Gold)** using **Parquet storage** and demonstrates core Spark data engineering concepts such as partitioning, broadcast joins, and distributed ETL processing.

---

# Architecture

Raw Dataset  
↓  
Bronze Layer (Raw Ingestion)  
↓  
Silver Layer (Cleaned & Enriched Data)  
↓  
Gold Layer (Analytics Tables)

### Bronze
Raw ingestion layer where datasets are standardized and stored in partitioned Parquet format.

### Silver
Cleaned and validated datasets with additional derived features.

### Gold
Aggregated analytics tables optimized for querying and reporting.

---

# Tech Stack

- PySpark
- Apache Spark
- Docker
- Parquet
- Python

---

# Dataset

NYC Taxi Trip Dataset  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

The pipeline processes multiple months of taxi trip records
