"""Data quality and deduplication logic for silver layer."""

from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


def apply_quality_filters(
    dataframe: DataFrame,
    distance_col: str,
    fare_col: str,
    min_trip_miles: float,
    max_trip_miles: float,
    min_trip_seconds: int,
    max_trip_seconds: int,
    min_fare_amount: float,
    max_fare_amount: float,
) -> DataFrame:
    """Filter rows outside configured business-valid ranges."""
    filtered = dataframe
    if "trip_duration_sec" in filtered.columns:
        filtered = filtered.filter(
            col("trip_duration_sec").between(min_trip_seconds, max_trip_seconds)
        )
    if distance_col in filtered.columns:
        filtered = filtered.filter(col(distance_col).between(min_trip_miles, max_trip_miles))
    if fare_col in filtered.columns:
        filtered = filtered.filter(col(fare_col).between(min_fare_amount, max_fare_amount))
    return filtered


def drop_null_critical(dataframe: DataFrame, critical_columns: List[str]) -> DataFrame:
    """Drop rows with null values in critical columns."""
    available = [c for c in critical_columns if c in dataframe.columns]
    if not available:
        return dataframe
    condition = " AND ".join(["{0} IS NOT NULL".format(c) for c in available])
    return dataframe.where(condition)


def deduplicate(
    dataframe: DataFrame,
    dedup_keys: List[str],
    dedup_order_col: str,
    strategy: str = "drop_duplicates",
) -> DataFrame:
    """Deduplicate records using key set and selected strategy.

    Supported strategies:
    - drop_duplicates: hash-based dedup (lower memory footprint)
    - window_keep_latest: keep latest row by dedup_order_col (heavier sort)
    """
    keys = [k for k in dedup_keys if k in dataframe.columns]
    if not keys:
        return dataframe

    if strategy == "drop_duplicates":
        return dataframe.dropDuplicates(keys)

    order_col = dedup_order_col if dedup_order_col in dataframe.columns else keys[0]
    window_spec = Window.partitionBy(*keys).orderBy(col(order_col).desc_nulls_last())
    return (
        dataframe
        .withColumn("_rn", row_number().over(window_spec))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )


def apply_temporal_consistency(
    dataframe: DataFrame,
    pickup_col: str,
    dropoff_col: str,
    source_duration_col: str,
    max_duration_delta_sec: int,
) -> DataFrame:
    """Enforce timestamp ordering and source duration consistency."""
    checked = dataframe

    if pickup_col in checked.columns and dropoff_col in checked.columns:
        checked = checked.filter(col(dropoff_col) >= col(pickup_col))

    if "trip_duration_sec" in checked.columns and source_duration_col in checked.columns:
        checked = checked.filter(
            spark_abs(col("trip_duration_sec") - col(source_duration_col))
            <= max_duration_delta_sec
        )

    return checked
