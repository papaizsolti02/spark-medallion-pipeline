"""Data writing operations for bronze layer."""

from typing import Optional

from pyspark.sql import DataFrame


def write_bronze(
    dataframe: DataFrame,
    bronze_path: str,
    partition_col: str,
    write_mode: str = "overwrite",
    compression: str = "none",
    max_records_per_file: int = 5_000_000,
) -> None:
    """Write DataFrame to bronze layer in Parquet format.

    Args:
        dataframe: Processed DataFrame to write.
        bronze_path: Target path for bronze data.
        partition_col: Column to partition by.
        write_mode: Spark write mode (overwrite, append).
        compression: Compression codec (none, snappy).
        max_records_per_file: Max records per output file.
    """
    (
        dataframe.write
        .mode(write_mode)
        .partitionBy(partition_col)
        .option("compression", compression)
        .option("maxRecordsPerFile", max_records_per_file)
        .parquet(bronze_path)
    )


def get_write_mode(is_first_write: bool) -> str:
    """Determine appropriate write mode based on whether this is initial write.

    Args:
        is_first_write: True if this is the first write operation.

    Returns:
        Spark write mode string.
    """
    return "overwrite" if is_first_write else "append"
