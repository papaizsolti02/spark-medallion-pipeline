# Spark Medallion Pipeline

Production-ready Apache Spark data pipeline implementing the Medallion Architecture (Bronze, Silver, Gold layers) for large-scale NYC taxi trip data processing. Built with PySpark 3.5, Docker, and optimized for batch processing on local and distributed systems.

## Architecture Overview

The pipeline follows the medallion architecture pattern, where data flows through three layers with progressive enrichment and refinement:

```
Raw Data (Parquet)
    |
    v
Bronze Layer
- Raw ingestion
- Schema validation
- Data quality filtering
- Monthly partitioning
    |
    v
Silver Layer
- Data cleaning
- Deduplication
- Derived features
    |
    v
Gold Layer
- Business-ready aggregations
- Analytics tables
- Query optimization
```

### Layer Specifications

Bronze: Ingests raw data from source Parquet files with monthly partitioning by pickup_month. Applies schema validation and data quality filters to remove invalid records before persisting to columnar format.

Silver: Applies transformations including deduplication, derived column generation, and cleansing rules. Data integrity is maintained through validation framework. Typically used for department-specific analytics.

Gold: Contains pre-computed aggregations optimized for BI tool consumption and reporting layers. All data is fully validated and quality-assured for production use.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Python 3.8+ (for local development)
- 8GB+ available memory

### Running the Pipeline

1. Build and start containers:
```bash
docker compose up --build
```

2. Run the pipeline with default configuration:
```bash
docker exec spark-medallion python3 -m scripts.run_pipeline
```

3. Monitor execution:
View Spark UI at http://localhost:4040

### Running Benchmarks

Execute Bronze benchmark suite:

```bash
docker exec spark-medallion python3 -m scripts.benchmark_spark_configs
```

Execute Silver benchmark suite:

```bash
docker exec spark-medallion python3 -m scripts.benchmark_silver_configs
```

Options:
- `--processing-mode`: full_batch or file_loop
- `--shuffle-partitions`: Comma-separated list (12,24)
- `--coalesce`: Partition count after coalescing (8,12)
- `--driver-memory`: Driver heap size (8g,10g)
- `--executor-memory`: Executor heap size (3g,4g)
- `--file-limit`: Maximum files to process

## Project Structure

```
spark-medallion-pipeline/
├── src/
│   ├── core/
│   │   ├── config_loader.py         Configuration management
│   │   └── spark.py                  Spark factory with tuned settings
│   ├── jobs/
│   │   ├── bronze/
│   │   │   ├── raw_to_bronze.py        Bronze ingestion orchestrator
│   │   │   ├── bronze_writers.py       Bronze write operations
│   │   │   ├── bronze_transformers.py  Bronze feature columns
│   │   │   └── file_discovery.py       Bronze file listing utilities
│   │   ├── silver/
│   │   │   ├── bronze_to_silver.py     Silver orchestration job
│   │   │   ├── silver_readers.py       Silver read utilities
│   │   │   ├── silver_transformers.py  Silver feature engineering
│   │   │   ├── silver_validators.py    Silver quality + dedup rules
│   │   │   └── silver_writers.py       Silver write utilities
│   │   └── gold/
│   ├── pipelines/
│   │   └── pipeline_runner.py       Pipeline orchestration
│   ├── utils/
│   │   ├── logger.py                Structured logging
│   │   ├── benchmark_utils.py       Benchmark utilities
│   │   ├── benchmark_results.py     Results management
│   │   └── jvm_manager.py           JVM lifecycle
│   └── validation/
│       └── data_quality.py          Validation rules
├── scripts/
│   ├── run_pipeline.py              Pipeline entry point
│   ├── benchmark_spark_configs.py   Bronze benchmark orchestrator
│   └── benchmark_silver_configs.py  Silver benchmark orchestrator
├── configs/
│   └── pipeline_config.yaml         Pipeline configuration
├── data/
│   ├── raw/                          Source data
│   ├── bronze/                       Bronze layer output
│   ├── silver/                       Silver layer output
│   └── gold/                         Gold layer output
├── docker-compose.yml               Container orchestration
└── Dockerfile                       Container specification
```

## Configuration

Pipeline behavior is controlled through `configs/pipeline_config.yaml`:

### Spark Settings

```yaml
spark:
  shuffle_partitions: 12           # Shuffle operation parallelism
  executor_cores: 3                # Cores per executor
  executor_memory: 2g              # Executor heap memory
  driver_memory: 6g                # Driver heap memory
  max_partition_bytes: 256MB       # Max bytes per partition (Parquet read)
```

### Bronze Job Configuration

```yaml
bronze_job:
  processing_mode: file_loop       # file_loop or full_batch
  coalesce_n: 8                    # Partitions after coalesce
  compression: snappy              # none or snappy
  max_records_per_file: 5000000    # Records per output partition file
  file_limit: null                 # Process all files (or limit to N)
  count_rows: false                # Count records during ingestion
```

### Silver Job Configuration

```yaml
silver_job:
  enabled: true
  source_path: /app/data/bronze/taxi_trips
  target_path: /app/data/silver/taxi_trips
  processing_mode: file_loop       # recommended locally; full_batch is heavier
  partition_column: pickup_month
  pickup_ts_col: pickup_datetime
  dropoff_ts_col: dropoff_datetime
  distance_col: trip_miles
  fare_col: base_passenger_fare
  critical_not_null: pickup_datetime,dropoff_datetime,pickup_month,trip_miles
  dedup_keys: hvfhs_license_num,pickup_datetime,dropoff_datetime,PULocationID,DOLocationID
  dedup_order_col: ingestion_timestamp
  coalesce_n: 8
  compression: snappy
```

### Benchmark Configuration

```yaml
benchmark:
  shuffle_partitions: "12,16,24"   # Test values
  coalesce: "8,12"
  driver_memory: "10g"
  executor_memory: "3g,4g"
  cores: "3,4"
  max_partition_bytes: "128MB,256MB"
  compression: "none,snappy"
  processing_mode: full_batch
  output: data/benchmark/spark_config_benchmark.csv

silver_benchmark:
  shuffle_partitions: "12,24"
  coalesce: "8,16"
  driver_memory: "6g"
  executor_memory: "2g,3g"
  cores: "2,3"
  max_partition_bytes: "128MB,256MB"
  compression: "snappy,none"
  processing_mode: full_batch
  output: data/benchmark/silver_config_benchmark.csv
```

## Processing Modes

### File Loop Mode

Processes source files sequentially, one at a time. Each file is read, transformed, and written to bronze independently.

Advantages:
- Lower memory footprint per iteration
- Better for I/O-bound workloads
- Fault isolation (single file failure does not affect pipeline)

Disadvantages:
- More overhead from multiple Spark jobs
- Slower for small-to-medium data volumes
- Risk of resource exhaustion at high coalesce factors

### Full Batch Mode

Reads all source files in a single Spark job, applies transformations, and writes results atomically.

Advantages:
- Single Spark job overhead
- Better query optimization across all data
- Atomic write semantics

Disadvantages:
- Higher memory requirements
- All-or-nothing semantics (fail if any file is problematic)
- Less fault tolerant

## Performance Tuning

### Optimal Configuration (from benchmarks)

For production use, the following configuration balances speed and stability:

```yaml
spark:
  shuffle_partitions: 12
  executor_cores: 3
  executor_memory: 3g
  driver_memory: 10g
  max_partition_bytes: 256MB

bronze_job:
  processing_mode: file_loop
  coalesce_n: 8
  compression: snappy
```

Expected performance: 235-250 seconds for 2 files (NYC taxi trips)

### Key Tuning Parameters

shuffle_partitions: Control task parallelism during shuffle operations. Higher values improve parallelism but increase scheduling overhead. Optimal range: 12-24.

coalesce_n: Reduce output partition count post-transformation. Lower values (8-12) are recommended. Values above 12 risk memory pressure and resource exhaustion.

executor_memory: 3GB is optimal for this workload. Higher values (4GB+) introduce garbage collection pressure with minimal performance gain.

driver_memory: Minimum 10GB for managing task scheduling and metadata. 8GB causes measurable performance degradation.

compression: Snappy compression has negligible performance impact and reduces storage footprint. Recommended for all deployments.

## Data Validation

All ingestion is subject to data quality checks in `src/validation/data_quality.py`. Current rules:

- trip_miles > 0
- trip_time > 0

Invalid records are silently filtered. Future validation framework should support configurable rules, metrics collection, and alerting.

## Monitoring and Logging

Structured JSON logging is enabled for all components. Logs include:

- Timestamp (UTC ISO 8601 format)
- Log level
- Logger name and module
- Message
- Function and line number
- Exception details (if applicable)
- Optional contextual fields (job, layer, run_id, file_name)

Enable JSON format by setting:
```bash
export LOG_FORMAT=json
```

Default is human-readable text format suitable for development.

## Development and Testing

### Running Locally

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run pipeline:
```bash
python3 -m scripts.run_pipeline
```

3. Run benchmarks:
```bash
python3 -m scripts.benchmark_spark_configs
```

### Type Checking

All code is fully type-hinted using Python 3.9+ syntax. Validate with:
```bash
pip install mypy
mypy src/ scripts/
```

## Technology Stack

Component | Technology | Version
--- | --- | ---
Compute Engine | Apache Spark | 3.5.0
Language | Python | 3.8+
Storage Format | Apache Parquet | -
Containerization | Docker | Latest
Orchestration | Docker Compose | 3.8
Configuration | YAML | -

### Dependencies

pyspark (3.5.0): Distributed data processing
pandas: Local data manipulation
pyarrow: Parquet I/O optimization
pyyaml: Configuration parsing
requests: HTTP utilities
tqdm: Progress bars

## Troubleshooting

### Out of Memory Errors

Reduce coalesce_n or executor_memory. Check Spark UI for stage details.

### Parquet Merge Schema Errors

Ensure source files have identical schemas. Use `spark.sql.files.maxPartitionBytes` to control partition scanning.

### Executor Disconnection

Indicates resource exhaustion or long GC pause. Reduce coalesce_n, increase executor_memory, or switch to full_batch mode.

### Connection Refused

JVM process crashed. Check system resources and Spark logs. Automatic recovery via JVM manager will retry the trial.

## Production Deployment

1. Use file_loop mode with coalesce_n=8 as baseline
2. Implement fallback to full_batch mode on failure
3. Deploy with monitoring for execution time and error rates
4. Log all executions to centralized system
5. Set up alerting for regressions in baseline performance
6. Schedule incremental runs at appropriate intervals (daily/hourly depending on data volume)

## Contributing

Code standards:

- 100% type hint coverage
- All public functions have docstrings
- Maximum 100 lines per module (split when necessary)
- All modules under src/ must be imported directly
- Logs use structured format with contextual information

When adding features:

1. Add comprehensive docstrings
2. Include type hints
3. Update configuration reference
4. Add benchmark trials if modifying transformations
5. Update this README

## License

Internal use only.
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
- Driver memory: `6GB`
- Executor memory: `2GB`
- Shuffle partitions: `12`
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
