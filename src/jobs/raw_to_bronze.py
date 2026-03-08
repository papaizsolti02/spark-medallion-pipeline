import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from src.validation.data_quality import validate_taxi_data


def run(
    spark: SparkSession,
    config: dict):

    raw_path = config["raw"]["path"]
    bronze_path = config["bronze"]["path"] + "/taxi_trips"
    partition_col = config["bronze"]["partition_column"]

    # List all parquet files in raw directory
    raw_files = sorted([f for f in os.listdir(raw_path) if f.endswith('.parquet')])
    
    if not raw_files:
        print("No parquet files found in raw directory!")
        return
    
    print(f"Found {len(raw_files)} files to process")
    
    is_first_write = True
    
    for idx, file in enumerate(raw_files, 1):
        file_path = os.path.join(raw_path, file)
        print(f"\n[{idx}/{len(raw_files)}] Processing {file}...")
        
        # Read single file
        df = spark.read.option("mergeSchema", "false").parquet(file_path)
        print(f"  - Data read ({df.count()} rows)")

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

        df = df.coalesce(8)

        write_mode = "overwrite" if is_first_write else "append"
        
        print(f"  - Writing to bronze ({write_mode} mode)...")
        
        (
            df.write
            .mode(write_mode)
            .partitionBy(partition_col)
            .option("compression", "none")
            .option("maxRecordsPerFile", 5_000_000)
            .parquet(bronze_path)
        )
        
        is_first_write = False
        print(f"  - {file} complete!")

    print("\nBronze ingestion complete!")