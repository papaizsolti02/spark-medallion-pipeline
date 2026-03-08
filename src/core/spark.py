import os
from pyspark.sql import SparkSession


def create_spark(
    app_name: str = "spark-medallion-pipeline",
    shuffle_partitions: int = 24,
    executor_instances: int = 4,
    cores: int = 4,
    memory: str = "3g",
    enable_arrow: bool = True,
    adaptive_execution: bool = True,
    coalesce_partitions: bool = True
) -> SparkSession:

    master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(master)

        # executor configuration
        .config("spark.executor.instances", executor_instances)
        .config("spark.executor.cores", cores)
        .config("spark.executor.memory", memory)

        # executor overhead
        .config("spark.executor.memoryOverhead", "512m")

        # driver
        .config("spark.driver.memory", "8g")

        # shuffle tuning
        .config("spark.sql.shuffle.partitions", shuffle_partitions)

        # arrow
        .config("spark.sql.execution.arrow.pyspark.enabled", enable_arrow)

        # adaptive execution
        .config("spark.sql.adaptive.enabled", adaptive_execution)
        .config("spark.sql.adaptive.coalescePartitions.enabled", coalesce_partitions)

        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")

        # parquet tuning
        .config("spark.sql.parquet.enableDictionary", "false")
        .config("spark.sql.files.maxPartitionBytes", "256MB")

        # better shuffle handling
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        .config("spark.sql.parquet.enableDictionary", "false")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
