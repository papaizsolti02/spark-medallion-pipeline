# Spark Medallion Pipeline

A production-ready PySpark data pipeline implementing the **Medallion Architecture** (Bronze → Silver → Gold) for processing large-scale NYC Taxi trip data (~100M+ rows). Built with Apache Spark 3.5, Docker, and optimized for high-throughput batch processing on local and distributed systems.

## 🏗️ Architecture

```
Raw Data (Parquet)
    ↓
┌────────────────────────┐
│   Bronze Layer         │  ← Raw Ingestion (Partitioned by Month)
│   - Standardized       │
│   - Validated          │
│   - Partitioned        │
└────────────────────────┘
    ↓
┌────────────────────────┐
│   Silver Layer         │  ← Cleaned & Enriched
│   - Cleaned            │
│   - Deduplicated       │
│   - Feature Eng        │
└────────────────────────┘
    ↓
┌────────────────────────┐
│   Gold Layer           │  ← Business-Ready Analytics
│   - Aggregated         │
│   - Optimized Queries  │
│   - Reports            │
└────────────────────────┘
```

### Layer Descriptions

| Layer | Purpose | Features |
|-------|---------|----------|
| **Bronze** | Raw data ingestion from source Parquet files | • Monthly partitioning<br>• Schema validation<br>• Data quality filters<br>• Incremental file processing |
| **Silver** | Cleaned, enriched dataset ready for analytics | • Deduplication<br>• Derived columns<br>• Business logic transformations |
| **Gold** | Aggregated, business-ready analytics tables | • Pre-computed aggregations<br>• Optimized for BI tools<br>• Query performance tuning |

---

## 🚀 Features

- ✅ **Medallion Architecture** - Industry-standard data lakehouse pattern
- ✅ **Incremental Processing** - File-by-file processing to optimize memory and I/O
- ✅ **Monthly Partitioning** - Efficient partition pruning for downstream queries
- ✅ **Adaptive Query Execution** - Spark AQE enabled for dynamic optimization
- ✅ **Data Quality Validation** - Built-in validation rules at ingestion
- ✅ **Docker-Based** - Reproducible environment with bind mount support
- ✅ **Configurable** - YAML-based configuration for all pipeline parameters
- ✅ **Spark UI Monitoring** - Real-time job monitoring at `localhost:4040`

---

## 🛠️ Tech Stack

| Component | Technology | Version |
|-----------|-----------|---------|
| **Compute Engine** | Apache Spark | 3.5.0 |
| **Language** | PySpark + Python | 3.8+ |
| **Storage Format** | Apache Parquet | - |
| **Containerization** | Docker | Latest |
| **Orchestration** | Docker Compose | 3.8 |
| **Config Management** | YAML | - |

**Key Libraries:**
- `pyspark==3.5.0` - Distributed data processing
- `pandas` - Data manipulation
- `pyarrow` - Parquet I/O optimization
- `pyyaml` - Configuration management

---

## 📦 Project Structure

```
spark-medallion-pipeline/
├── configs/
│   └── pipeline_config.yaml       # Pipeline configuration
├── data/
│   ├── raw/                        # Source Parquet files
│   ├── bronze/                     # Bronze layer output
│   ├── silver/                     # Silver layer output (WIP)
│   └── gold/                       # Gold layer output (WIP)
├── src/
│   ├── core/
│   │   ├── spark.py               # Spark session factory
│   │   └── config_loader.py       # Config loader utility
│   ├── jobs/
│   │   └── raw_to_bronze.py       # Bronze ingestion job
│   ├── pipelines/
│   │   └── pipeline_runner.py     # Main pipeline orchestrator
│   ├── utils/
│   │   └── logger.py              # Logging utilities
│   └── validation/
│       └── data_quality.py        # Data validation rules
├── scripts/
│   └── run_pipeline.py            # Pipeline entry point
├── docker-compose.yml             # Docker orchestration
├── Dockerfile                     # Container definition
├── requirements.txt               # Python dependencies
└── README.md                      # This file
```

---

## ⚙️ Configuration

Edit `configs/pipeline_config.yaml` to customize pipeline behavior:

```yaml
raw:
  path: /app/data/raw

bronze:
  path: /app/data/bronze
  partition_column: pickup_month

silver:
  path: /app/data/silver

gold:
  path: /app/data/gold
```

**Spark Configuration** (`src/core/spark.py`):
- Driver memory: `8GB`
- Shuffle partitions: `24`
- Max partition bytes: `256MB`
- Adaptive execution: `enabled`

---

## 🏃 Getting Started

### Prerequisites

- Docker & Docker Compose
- At least 8GB RAM available for Docker
- NYC Taxi parquet files in `data/raw/`

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd spark-medallion-pipeline
   ```

2. **Place raw data files**
   ```bash
   # Copy your parquet files to data/raw/
   cp /path/to/*.parquet data/raw/
   ```

3. **Build and start the container**
   ```bash
   docker compose up -d --build
   ```

4. **Run the bronze ingestion pipeline**
   ```bash
   docker exec -it spark-medallion python3 -m scripts.run_pipeline
   ```

5. **Monitor progress**
   - View logs in terminal
   - Open Spark UI: http://localhost:4040

---

## 📊 Dataset

**NYC Taxi Trip Records**  
Source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

The pipeline processes high-volume For-Hire Vehicle (FHV) trip data with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| `pickup_datetime` | Timestamp | Trip start time |
| `dropoff_datetime` | Timestamp | Trip end time |
| `trip_miles` | Double | Distance traveled |
| `trip_time` | Integer | Duration in seconds |
| ... | ... | Additional fields |

**Processing Volume:** ~100M+ rows across multiple monthly files (~450MB each compressed)

---

## 🔧 Performance Optimizations

### 1. Incremental File Processing
Instead of loading all files at once (memory intensive), the pipeline processes **one file at a time** in a loop:
- Reduced memory footprint from 15-20GB to ~2-3GB per iteration
- Eliminates OOM errors on resource-constrained systems
- First file overwrites, subsequent files append

### 2. Partitioning Strategy
- **Read:** 24 partitions (driven by `spark.sql.files.maxPartitionBytes=256MB`)
- **Write:** 8 coalesced partitions to reduce I/O contention on bind mounts
- **Output:** Monthly partitions via `.partitionBy("pickup_month")` for efficient query pruning

### 3. Compression Trade-offs
- **No compression** on Bronze layer for faster writes on local development (WSL2/bind mounts)
- Compression can be re-enabled for production cloud deployments (Snappy/Gzip)

### 4. Adaptive Query Execution
```python
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true
```
Spark dynamically optimizes partition sizes and handles data skew automatically.

---

## 📈 Pipeline Performance

| Metric | Value |
|--------|-------|
| **Data Volume** | 5.6 GB compressed (12 files) |
| **Total Runtime** | ~6-12 minutes |
| **Per-File Processing** | ~30-60 seconds |
| **Read Throughput** | ~100 MB/s |
| **Write Throughput** | ~50-80 MB/s (WSL2 bind mount) |

**Note:** Performance varies based on:
- Host system specs (CPU, RAM, disk I/O)
- WSL2 vs. native Linux (native ~2x faster)
- Compression settings
- Number of partitions

---

## 🐛 Troubleshooting

### Out of Memory Errors
```bash
# Increase driver memory in src/core/spark.py
.config("spark.driver.memory", "12g")  # Increase from 8g
```

### Slow Write Performance
```bash
# Reduce coalesce partitions in src/jobs/raw_to_bronze.py
df = df.coalesce(4)  # Try 2 or 4 instead of 8
```

### Container Not Starting
```bash
# Check logs and rebuild
docker compose logs
docker compose down -v
docker compose up -d --build
```

---

## 🔮 Roadmap

- [ ] Implement Silver layer transformations
- [ ] Implement Gold layer aggregations
- [ ] Add incremental processing with watermarking
- [ ] Add automated data quality tests
- [ ] Support for streaming ingestion (Structured Streaming)
- [ ] CI/CD pipeline integration
- [ ] Cloud deployment configurations (AWS EMR, Azure Databricks)

---

## 📝 License

This project is licensed under the MIT License.

---

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for open data
- Apache Spark community
- Databricks for Medallion Architecture best practices
