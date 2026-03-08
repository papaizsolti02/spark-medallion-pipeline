from src.core.spark import create_spark
from src.core.config_loader import load_config

from src.jobs.raw_to_bronze import run as raw_bronze

def run_pipeline():

    config = load_config("configs/pipeline_config.yaml")

    spark = create_spark(
        "spark-medallion-pipeline",
    )
    
    print("Spark master:", spark.sparkContext.master)
    print("Parallelism:", spark.sparkContext.defaultParallelism)

    raw_bronze(spark, config)

    spark.stop()