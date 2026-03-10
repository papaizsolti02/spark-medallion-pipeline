"""Data transformation logic for bronze layer ingestion."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    current_timestamp,
    date_format,
    input_file_name,
    to_date,
)


def apply_derived_columns(dataframe: DataFrame) -> DataFrame:
    """Apply derived column transformations to raw ingestion data.

    Args:
        dataframe: Raw input DataFrame.

    Returns:
        DataFrame with derived columns added.
    """
    return (
        dataframe
        .withColumn("pickup_date", to_date("pickup_datetime"))
        .withColumn("pickup_month", date_format("pickup_datetime", "yyyy-MM"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )
