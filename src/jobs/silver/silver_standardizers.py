"""Type normalization and canonicalization for silver processing."""

from typing import Any, Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, to_timestamp, trim, upper, when


def standardize_source_types(dataframe: DataFrame, job_cfg: Dict[str, Any]) -> DataFrame:
    """Normalize source data types and add canonical helper columns.

    Args:
        dataframe: Input DataFrame from bronze.
        job_cfg: Silver job configuration.

    Returns:
        DataFrame with normalized dtypes and helper boolean columns.
    """
    pickup_col = str(job_cfg.get("pickup_ts_col", "pickup_datetime"))
    dropoff_col = str(job_cfg.get("dropoff_ts_col", "dropoff_datetime"))
    distance_col = str(job_cfg.get("distance_col", "trip_miles"))
    fare_col = str(job_cfg.get("fare_col", "base_passenger_fare"))
    partition_col = str(job_cfg.get("partition_column", "pickup_month"))

    timestamp_cols = ["request_datetime", "on_scene_datetime", pickup_col, dropoff_col]
    numeric_double_cols = [
        distance_col,
        fare_col,
        "tolls",
        "bcf",
        "sales_tax",
        "congestion_surcharge",
        "airport_fee",
        "tips",
        "driver_pay",
        "cbd_congestion_fee",
    ]
    numeric_int_cols = ["trip_time", "PULocationID", "DOLocationID"]
    flag_cols = [
        "shared_request_flag",
        "shared_match_flag",
        "access_a_ride_flag",
        "wav_request_flag",
        "wav_match_flag",
    ]

    normalized = dataframe
    for col_name in timestamp_cols:
        if col_name in normalized.columns:
            normalized = normalized.withColumn(col_name, to_timestamp(col(col_name)))

    for col_name in numeric_double_cols:
        if col_name in normalized.columns:
            normalized = normalized.withColumn(col_name, col(col_name).cast("double"))

    for col_name in numeric_int_cols:
        if col_name in normalized.columns:
            normalized = normalized.withColumn(col_name, col(col_name).cast("int"))

    for col_name in flag_cols:
        if col_name in normalized.columns:
            normalized = normalized.withColumn(
                col_name + "_bool",
                when(upper(trim(col(col_name))) == "Y", True)
                .when(upper(trim(col(col_name))) == "N", False)
                .otherwise(None),
            )

    if partition_col not in normalized.columns and pickup_col in normalized.columns:
        normalized = normalized.withColumn(partition_col, date_format(col(pickup_col), "yyyy-MM"))

    return normalized
