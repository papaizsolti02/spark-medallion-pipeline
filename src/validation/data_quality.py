"""Data quality validation rules for taxi data."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def validate_taxi_data(dataframe: DataFrame) -> DataFrame:
    """Apply data quality filters to taxi trip data.

    Removes records with invalid or suspicious values:
    - trip_miles must be greater than 0
    - trip_time must be greater than 0

    Args:
        dataframe: Raw taxi trip DataFrame.

    Returns:
        Filtered DataFrame with valid records only.
    """
    return (
        dataframe
        .filter(col("trip_miles") > 0)
        .filter(col("trip_time") > 0)
    )