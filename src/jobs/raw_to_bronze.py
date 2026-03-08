from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.validation.data_quality import validate_taxi_data


def run(
    spark: SparkSession,
    config: dict):

    raw_path = config["raw"]["path"]
    bronze_path = config["bronze"]["path"]
    partition_col = config["bronze"]["partition_column"]

    df = spark.read.parquet(raw_path)
    
    print("Data read!")

    # data quality checks
    df = validate_taxi_data(df)

    # metadata columns
    df = (
        df
        .withColumn("pickup_date", to_date("pickup_datetime"))
        .withColumn("pickup_month", date_format("pickup_datetime", "yyyy-MM"))
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", input_file_name())
    )

    df = df.repartition(64, partition_col)

    print("Data quality checks passed, writing to bronze...")

    (
        df.write
        .mode("overwrite")
        .partitionBy(partition_col)
        .option("compression", "snappy")
        .parquet(bronze_path)
    )
