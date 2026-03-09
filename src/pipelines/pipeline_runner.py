from src.core.spark import create_spark
from src.core.config_loader import load_config
from src.jobs.raw_to_bronze import run as raw_bronze
from src.utils.logger import get_logger


logger = get_logger(__name__)

def run_pipeline():

    config = load_config("configs/pipeline_config.yaml")
    spark_config = config.get("spark", {})
    bronze_job_config = config.get("bronze_job", {})

    spark = create_spark(
        "spark-medallion-pipeline",
        shuffle_partitions=int(spark_config.get("shuffle_partitions", 24)),
        cores=int(spark_config.get("executor_cores", 3)),
        memory=str(spark_config.get("executor_memory", "3g")),
        driver_memory=str(spark_config.get("driver_memory", "8g")),
        max_partition_bytes=str(spark_config.get("max_partition_bytes", "256MB")),
    )
    
    logger.info("Spark master: %s", spark.sparkContext.master)
    logger.info("Parallelism: %s", spark.sparkContext.defaultParallelism)

    raw_bronze(
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

    spark.stop()