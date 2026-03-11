"""Main pipeline orchestration."""

from typing import Any, Dict

from pyspark.sql import SparkSession

from src.core.config_loader import load_config
from src.core.spark import create_spark
from src.jobs.bronze.raw_to_bronze import run as raw_to_bronze
from src.jobs.silver.bronze_to_silver import run as bronze_to_silver
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_pipeline() -> None:
    """Execute the complete medallion architecture pipeline.

    Loads configuration, creates Spark session, and executes raw-to-bronze
    ingestion job with configured parameters.

    Configuration is loaded from configs/pipeline_config.yaml and includes:
    - Spark session parameters (memory, cores, partitions)
    - Data paths (raw, bronze, silver, gold)
    - Job-specific parameters (compression, coalesce, processing mode)
    """
    config: Dict[str, Any] = load_config("configs/pipeline_config.yaml")
    spark_config: Dict[str, Any] = config.get("spark", {})
    bronze_job_config: Dict[str, Any] = config.get("bronze_job", {})
    silver_job_config: Dict[str, Any] = config.get("silver_job", {})

    spark: SparkSession = create_spark(
        app_name="spark-medallion-pipeline",
        shuffle_partitions=int(spark_config.get("shuffle_partitions", 24)),
        cores=int(spark_config.get("executor_cores", 3)),
        memory=str(spark_config.get("executor_memory", "3g")),
        driver_memory=str(spark_config.get("driver_memory", "8g")),
        max_partition_bytes=str(spark_config.get("max_partition_bytes", "256MB")),
    )

    logger.info("Spark master: %s", spark.sparkContext.master)
    logger.info("Parallelism: %s", spark.sparkContext.defaultParallelism)

    raw_to_bronze(
        spark,
        config,
        processing_mode=str(bronze_job_config.get("processing_mode", "full_batch")),
        coalesce_n=int(bronze_job_config.get("coalesce_n", 12)),
        compression=str(bronze_job_config.get("compression", "none")),
        max_records_per_file=int(bronze_job_config.get("max_records_per_file", 5_000_000)),
        file_limit=bronze_job_config.get("file_limit"),
        count_rows=bool(bronze_job_config.get("count_rows", False)),
        logger=logger,
    )

    # Run Silver in a fresh Spark app to avoid long-lived JVM memory buildup
    # from the Bronze stage in local containerized execution.
    spark.stop()

    if bool(silver_job_config.get("enabled", False)):
        spark = create_spark(
            app_name="spark-medallion-pipeline-silver",
            shuffle_partitions=int(spark_config.get("shuffle_partitions", 24)),
            cores=int(spark_config.get("executor_cores", 3)),
            memory=str(spark_config.get("executor_memory", "3g")),
            driver_memory=str(spark_config.get("driver_memory", "8g")),
            max_partition_bytes=str(spark_config.get("max_partition_bytes", "256MB")),
        )
        bronze_to_silver(
            spark,
            config,
            processing_mode=str(silver_job_config.get("processing_mode", "full_batch")),
            coalesce_n=int(silver_job_config.get("coalesce_n", 16)),
            compression=str(silver_job_config.get("compression", "snappy")),
            max_records_per_file=int(silver_job_config.get("max_records_per_file", 5_000_000)),
            file_limit=silver_job_config.get("file_limit"),
            count_rows=bool(silver_job_config.get("count_rows", False)),
            logger=logger,
        )

    spark.stop()