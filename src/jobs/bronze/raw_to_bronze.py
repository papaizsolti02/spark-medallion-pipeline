"""Bronze layer ingestion job orchestration."""

from logging import Logger
from time import perf_counter
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.jobs.bronze.bronze_transformers import apply_derived_columns
from src.jobs.bronze.bronze_writers import get_write_mode, write_bronze
from src.jobs.bronze.file_discovery import get_file_paths, list_parquet_files
from src.validation.data_quality import validate_taxi_data


def run(
    spark: SparkSession,
    config: Dict[str, Any],
    processing_mode: str = "full_batch",
    coalesce_n: int = 12,
    compression: str = "none",
    max_records_per_file: int = 5_000_000,
    file_limit: Optional[int] = None,
    count_rows: bool = False,
    logger: Optional[Logger] = None,
) -> Dict[str, Any]:
    """Execute raw-to-bronze ingestion job.

    Args:
        spark: SparkSession instance.
        config: Pipeline configuration dictionary.
        processing_mode: 'file_loop' or 'full_batch'.
        coalesce_n: Number of partitions after coalesce.
        compression: Compression codec ('none' or 'snappy').
        max_records_per_file: Maximum records per output file.
        file_limit: Maximum number of files to process.
        count_rows: Whether to count rows during ingestion.
        logger: Logger instance.

    Returns:
        Dictionary with execution statistics.
    """

    def _log(message: str) -> None:
        """Helper to log with fallback to print."""
        if logger:
            logger.info(message)
        else:
            print(message)

    raw_path: str = config["raw"]["path"]
    bronze_path: str = config["bronze"]["path"] + "/taxi_trips"
    partition_col: str = config["bronze"]["partition_column"]
    job_start: float = perf_counter()

    raw_files = list_parquet_files(raw_path, file_limit)
    if not raw_files:
        _log("No parquet files found in raw directory")
        return {"files_processed": 0, "total_seconds": 0.0}

    _log(f"Found {len(raw_files)} files to process")
    per_file_stats: List[Dict[str, Any]] = []

    if processing_mode == "file_loop":
        _process_file_loop(
            spark,
            raw_files,
            raw_path,
            bronze_path,
            partition_col,
            coalesce_n,
            compression,
            max_records_per_file,
            count_rows,
            per_file_stats,
            _log,
        )
    else:
        _process_full_batch(
            spark,
            raw_files,
            raw_path,
            bronze_path,
            partition_col,
            coalesce_n,
            compression,
            max_records_per_file,
            count_rows,
            per_file_stats,
            _log,
        )

    total_elapsed = perf_counter() - job_start
    _log(f"Bronze ingestion complete in {total_elapsed:.2f}s")
    return {
        "files_processed": len(raw_files),
        "total_seconds": round(total_elapsed, 2),
        "per_file": per_file_stats,
    }


def _process_file_loop(
    spark: SparkSession,
    raw_files: List[str],
    raw_path: str,
    bronze_path: str,
    partition_col: str,
    coalesce_n: int,
    compression: str,
    max_records_per_file: int,
    count_rows: bool,
    per_file_stats: List[Dict[str, Any]],
    log_func: Any,
) -> None:
    """Process files one at a time (file_loop mode)."""
    is_first_write = True
    for idx, filename in enumerate(raw_files, 1):
        file_start = perf_counter()
        file_path = f"{raw_path}/{filename}"
        log_func(f"[{idx}/{len(raw_files)}] Processing {filename}")

        df = spark.read.option("mergeSchema", "false").parquet(file_path)
        if count_rows:
            log_func(f"Data read: {df.count()} rows")
        else:
            log_func("Data read")

        df = _transform_dataframe(df, coalesce_n)
        write_bronze(
            df,
            bronze_path,
            partition_col,
            get_write_mode(is_first_write),
            compression,
            max_records_per_file,
        )
        is_first_write = False
        elapsed = perf_counter() - file_start
        per_file_stats.append({"file": filename, "seconds": round(elapsed, 2)})
        log_func(f"{filename} complete in {elapsed:.2f}s")


def _process_full_batch(
    spark: SparkSession,
    raw_files: List[str],
    raw_path: str,
    bronze_path: str,
    partition_col: str,
    coalesce_n: int,
    compression: str,
    max_records_per_file: int,
    count_rows: bool,
    per_file_stats: List[Dict[str, Any]],
    log_func: Any,
) -> None:
    """Process all files in one Spark job (full_batch mode)."""
    batch_start = perf_counter()
    file_paths = get_file_paths(raw_path, raw_files)
    log_func("Processing all files in one Spark job (full_batch mode)")

    df = spark.read.option("mergeSchema", "false").parquet(*file_paths)
    if count_rows:
        log_func(f"Data read: {df.count()} rows")
    else:
        log_func("Data read")

    df = _transform_dataframe(df, coalesce_n)
    write_bronze(
        df,
        bronze_path,
        partition_col,
        "overwrite",
        compression,
        max_records_per_file,
    )
    elapsed = perf_counter() - batch_start
    per_file_stats.append({"file": "ALL_FILES", "seconds": round(elapsed, 2)})
    log_func(f"All files complete in {elapsed:.2f}s")


def _transform_dataframe(dataframe: DataFrame, coalesce_n: int) -> DataFrame:
    """Apply validation and transformation pipeline."""
    df = validate_taxi_data(dataframe)
    df = apply_derived_columns(df)
    return df.coalesce(coalesce_n)