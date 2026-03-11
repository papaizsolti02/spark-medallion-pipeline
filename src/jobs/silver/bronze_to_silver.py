"""Bronze-to-silver transformation job orchestration."""

from logging import Logger
from time import perf_counter
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession

from src.jobs.silver.silver_readers import (
    attach_partition_column,
    list_partition_paths,
    read_all,
    read_partition,
)
from src.jobs.silver.silver_standardizers import standardize_source_types
from src.jobs.silver.silver_transformers import apply_silver_features
from src.jobs.silver.silver_validators import (
    apply_temporal_consistency,
    apply_quality_filters,
    deduplicate,
    drop_null_critical,
)
from src.jobs.silver.silver_writers import write_silver


def run(
    spark: SparkSession,
    config: Dict[str, Any],
    processing_mode: str = "full_batch",
    coalesce_n: int = 16,
    compression: str = "snappy",
    max_records_per_file: int = 5_000_000,
    file_limit: Optional[int] = None,
    count_rows: bool = False,
    logger: Optional[Logger] = None,
) -> Dict[str, Any]:
    """Execute bronze-to-silver transformation pipeline."""

    def _log(message: str) -> None:
        if logger:
            logger.info(message)
        else:
            print(message)

    job_cfg = config.get("silver_job", {})
    source_path = str(job_cfg.get("source_path", "/app/data/bronze/taxi_trips"))
    target_path = str(job_cfg.get("target_path", "/app/data/silver/taxi_trips"))
    partition_col = str(job_cfg.get("partition_column", "pickup_month"))
    partition_prefix = partition_col + "="

    job_start = perf_counter()
    per_item_stats: List[Dict[str, Any]] = []

    if processing_mode == "file_loop":
        partitions = list_partition_paths(source_path, partition_prefix)
        if file_limit:
            partitions = partitions[:file_limit]

        if not partitions:
            _log("No bronze partitions found for silver processing")
            return {"partitions_processed": 0, "total_seconds": 0.0}

        is_first_write = True
        for idx, part_path in enumerate(partitions, 1):
            start = perf_counter()
            _log("[{0}/{1}] Processing {2}".format(idx, len(partitions), part_path))
            dataframe = read_partition(spark, part_path)
            dataframe = attach_partition_column(
                dataframe,
                partition_col,
                part_path,
                partition_prefix,
            )
            transformed = _apply_silver_pipeline(dataframe, job_cfg, coalesce_n)
            mode = "overwrite" if is_first_write else "append"
            write_silver(
                transformed,
                target_path,
                partition_col,
                mode,
                compression,
                max_records_per_file,
            )
            is_first_write = False
            elapsed = perf_counter() - start
            per_item_stats.append({"partition": part_path, "seconds": round(elapsed, 2)})
            _log("Partition complete in {0:.2f}s".format(elapsed))
    else:
        _log("Processing full bronze dataset in one silver transformation job")
        dataframe = read_all(spark, source_path)
        if count_rows:
            _log("Input rows: {0}".format(dataframe.count()))
        transformed = _apply_silver_pipeline(dataframe, job_cfg, coalesce_n)
        write_silver(
            transformed,
            target_path,
            partition_col,
            "overwrite",
            compression,
            max_records_per_file,
        )
        elapsed = perf_counter() - job_start
        per_item_stats.append({"partition": "ALL", "seconds": round(elapsed, 2)})

    total_elapsed = perf_counter() - job_start
    _log("Silver transformation complete in {0:.2f}s".format(total_elapsed))
    return {
        "partitions_processed": len(per_item_stats),
        "total_seconds": round(total_elapsed, 2),
        "per_partition": per_item_stats,
    }


def _apply_silver_pipeline(
    dataframe: DataFrame,
    job_cfg: Dict[str, Any],
    coalesce_n: int,
) -> DataFrame:
    pickup_col = str(job_cfg.get("pickup_ts_col", "pickup_datetime"))
    dropoff_col = str(job_cfg.get("dropoff_ts_col", "dropoff_datetime"))
    distance_col = str(job_cfg.get("distance_col", "trip_miles"))
    fare_col = str(job_cfg.get("fare_col", "base_passenger_fare"))
    dedup_keys = [
        k.strip() for k in str(job_cfg.get("dedup_keys", "")).split(",") if k.strip()
    ]
    critical_columns = [
        c.strip() for c in str(job_cfg.get("critical_not_null", "")).split(",") if c.strip()
    ]
    dedup_order_col = str(job_cfg.get("dedup_order_col", "ingestion_timestamp"))
    dedup_strategy = str(job_cfg.get("dedup_strategy", "drop_duplicates"))
    source_duration_col = str(job_cfg.get("source_duration_col", "trip_time"))
    max_duration_delta_sec = int(job_cfg.get("max_duration_delta_sec", 900))

    required_source_columns = [pickup_col, dropoff_col, distance_col, fare_col]
    missing_columns = [column for column in required_source_columns if column not in dataframe.columns]
    if missing_columns:
        raise ValueError(
            "Silver job configuration references missing source columns: {0}. Available columns: {1}".format(
                ", ".join(missing_columns),
                ", ".join(sorted(dataframe.columns)),
            )
        )

    transformed = standardize_source_types(dataframe, job_cfg)
    transformed = apply_silver_features(
        transformed,
        pickup_col,
        dropoff_col,
        distance_col,
        fare_col,
    )
    transformed = apply_temporal_consistency(
        transformed,
        pickup_col,
        dropoff_col,
        source_duration_col,
        max_duration_delta_sec,
    )
    transformed = apply_quality_filters(
        transformed,
        distance_col,
        fare_col,
        float(job_cfg.get("min_trip_miles", 0.01)),
        float(job_cfg.get("max_trip_miles", 200.0)),
        int(job_cfg.get("min_trip_seconds", 60)),
        int(job_cfg.get("max_trip_seconds", 14400)),
        float(job_cfg.get("min_fare_amount", 0.0)),
        float(job_cfg.get("max_fare_amount", 1000.0)),
    )
    transformed = drop_null_critical(transformed, critical_columns)
    transformed = deduplicate(
        transformed,
        dedup_keys,
        dedup_order_col,
        dedup_strategy,
    )
    return transformed.coalesce(coalesce_n)
