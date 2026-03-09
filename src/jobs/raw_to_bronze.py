import os
from time import perf_counter
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, date_format, input_file_name, to_date

from src.validation.data_quality import validate_taxi_data


def run(
    spark: SparkSession,
    config: dict,
    processing_mode: str = "full_batch",
    coalesce_n: int = 12,
    compression: str = "none",
    max_records_per_file: int = 5_000_000,
    file_limit: Optional[int] = None,
    count_rows: bool = False,
    logger: Optional[Any] = None,
) -> Dict[str, Any]:

    def log(message: str) -> None:
        if logger:
            logger.info(message)
        else:
            print(message)

    raw_path = config["raw"]["path"]
    bronze_path = config["bronze"]["path"] + "/taxi_trips"
    partition_col = config["bronze"]["partition_column"]
    job_start = perf_counter()

    # List all parquet files in raw directory
    raw_files = sorted([f for f in os.listdir(raw_path) if f.endswith('.parquet')])
    if file_limit:
        raw_files = raw_files[:file_limit]
    
    if not raw_files:
        log("No parquet files found in raw directory")
        return {"files_processed": 0, "total_seconds": 0.0}
    
    log(f"Found {len(raw_files)} files to process")
    per_file_stats = []

    def with_derived_columns(df):
        return (
            df
            .withColumn("pickup_date", to_date("pickup_datetime"))
            .withColumn("pickup_month", date_format("pickup_datetime", "yyyy-MM"))
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", input_file_name())
        )

    if processing_mode == "file_loop":
        is_first_write = True
        for idx, file in enumerate(raw_files, 1):
            file_start = perf_counter()
            file_path = os.path.join(raw_path, file)
            log(f"[{idx}/{len(raw_files)}] Processing {file}")

            df = spark.read.option("mergeSchema", "false").parquet(file_path)
            if count_rows:
                row_count = df.count()
                log(f"Data read: {row_count} rows")
            else:
                log("Data read")

            df = validate_taxi_data(df)
            df = with_derived_columns(df)
            df = df.coalesce(coalesce_n)

            write_mode = "overwrite" if is_first_write else "append"
            log(f"Writing to bronze ({write_mode} mode)")

            (
                df.write
                .mode(write_mode)
                .partitionBy(partition_col)
                .option("compression", compression)
                .option("maxRecordsPerFile", max_records_per_file)
                .parquet(bronze_path)
            )

            is_first_write = False
            elapsed = perf_counter() - file_start
            per_file_stats.append({"file": file, "seconds": round(elapsed, 2)})
            log(f"{file} complete in {elapsed:.2f}s")
    else:
        # Full batch mode performs one read/write job and is faster for full refreshes.
        file_paths = [os.path.join(raw_path, file) for file in raw_files]
        log("Processing all selected files in one Spark job (full_batch mode)")
        df = spark.read.option("mergeSchema", "false").parquet(*file_paths)

        if count_rows:
            row_count = df.count()
            log(f"Data read: {row_count} rows")
        else:
            log("Data read")

        df = validate_taxi_data(df)
        df = with_derived_columns(df)
        df = df.coalesce(coalesce_n)

        log("Writing to bronze (overwrite mode)")
        (
            df.write
            .mode("overwrite")
            .partitionBy(partition_col)
            .option("compression", compression)
            .option("maxRecordsPerFile", max_records_per_file)
            .parquet(bronze_path)
        )

        total_file_seconds = perf_counter() - job_start
        per_file_stats.append({"file": "ALL_FILES", "seconds": round(total_file_seconds, 2)})
        log(f"All files complete in {total_file_seconds:.2f}s")

    total_elapsed = perf_counter() - job_start
    log(f"Bronze ingestion complete in {total_elapsed:.2f}s")
    return {
        "files_processed": len(raw_files),
        "total_seconds": round(total_elapsed, 2),
        "per_file": per_file_stats,
    }