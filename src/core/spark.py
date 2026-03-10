"""Spark session factory with optimized configuration."""

import os

from pyspark.sql import SparkSession


def create_spark(
    app_name: str = "spark-medallion-pipeline",
    shuffle_partitions: int = 24,
    executor_instances: int = 4,
    cores: int = 3,
    memory: str = "3g",
    driver_memory: str = "8g",
    max_partition_bytes: str = "256MB",
    enable_arrow: bool = True,
    adaptive_execution: bool = True,
    coalesce_partitions: bool = True,
) -> SparkSession:
    """Create and configure a SparkSession with performance optimizations.

    Args:
        app_name: Application name for Spark UI.
        shuffle_partitions: Number of partitions for shuffle operations.
        executor_instances: Number of executor instances.
        cores: Cores per executor.
        memory: Executor memory (e.g., '3g').
        driver_memory: Driver memory (e.g., '8g').
        max_partition_bytes: Maximum bytes per partition for Parquet reads.
        enable_arrow: Enable PyArrow for columnar data transfer.
        adaptive_execution: Enable Spark Adaptive Query Execution.
        coalesce_partitions: Enable adaptive partition coalescing.

    Returns:
        Configured SparkSession.

    Notes:
        Configuration includes:
        - PyArrow acceleration for Python/PySpark integration
        - Adaptive Query Execution for runtime optimization
        - Skew join handling for unbalanced datasets
        - Parquet performance tuning
        - Memory overhead buffer for JVM operations
    """
    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # Executor configuration
        .config("spark.executor.instances", executor_instances)
        .config("spark.executor.cores", cores)
        .config("spark.executor.memory", memory)
        .config("spark.executor.memoryOverhead", "512m")
        # Driver configuration
        .config("spark.driver.memory", driver_memory)
        # Shuffle tuning
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        # PyArrow acceleration
        .config("spark.sql.execution.arrow.pyspark.enabled", enable_arrow)
        # Adaptive execution
        .config("spark.sql.adaptive.enabled", adaptive_execution)
        .config("spark.sql.adaptive.coalescePartitions.enabled", coalesce_partitions)
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
        # Parquet tuning
        .config("spark.sql.parquet.enableDictionary", "false")
        .config("spark.sql.files.maxPartitionBytes", max_partition_bytes)
        # Skew handling
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
