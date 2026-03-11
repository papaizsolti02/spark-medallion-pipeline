"""Write utilities for silver layer outputs."""

from pyspark.sql import DataFrame


def write_silver(
    dataframe: DataFrame,
    target_path: str,
    partition_col: str,
    write_mode: str,
    compression: str,
    max_records_per_file: int,
) -> None:
    """Write transformed silver dataframe to parquet."""
    (
        dataframe.write
        .mode(write_mode)
        .partitionBy(partition_col)
        .option("compression", compression)
        .option("maxRecordsPerFile", max_records_per_file)
        .parquet(target_path)
    )
