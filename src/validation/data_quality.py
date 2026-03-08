from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def validate_taxi_data(
    df: SparkSession
) -> SparkSession:
    
    df = df.filter(col("trip_miles") > 0)
    df = df.filter(col("trip_time") > 0)

    return df