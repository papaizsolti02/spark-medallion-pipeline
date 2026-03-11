"""Read utilities for bronze-to-silver processing."""

import os
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit


def list_partition_paths(source_path: str, partition_prefix: str) -> List[str]:
    """List partition directories from bronze path.

    Args:
        source_path: Bronze dataset root path.
        partition_prefix: Partition directory prefix (e.g., pickup_month=).

    Returns:
        Sorted full partition paths.
    """
    dirs = sorted(
        [
            d for d in os.listdir(source_path)
            if d.startswith(partition_prefix) and os.path.isdir(os.path.join(source_path, d))
        ]
    )
    return [os.path.join(source_path, d) for d in dirs]


def read_partition(spark: SparkSession, partition_path: str) -> DataFrame:
    """Read one bronze partition path as DataFrame."""
    return spark.read.option("mergeSchema", "false").parquet(partition_path)


def read_all(spark: SparkSession, source_path: str) -> DataFrame:
    """Read full bronze dataset as DataFrame."""
    return spark.read.option("mergeSchema", "false").parquet(source_path)


def attach_partition_column(
    dataframe: DataFrame,
    partition_col: str,
    partition_path: str,
    partition_prefix: str,
) -> DataFrame:
    """Attach partition value when reading a leaf partition path.

    Spark omits partition columns if reading only leaf partition directories.
    This restores the column from the path segment.
    """
    if partition_col in dataframe.columns:
        return dataframe

    path_tail = os.path.basename(partition_path)
    if not path_tail.startswith(partition_prefix):
        return dataframe

    partition_value = path_tail.replace(partition_prefix, "", 1)
    return dataframe.withColumn(partition_col, lit(partition_value))
