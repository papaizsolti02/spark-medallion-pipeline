"""Silver layer feature engineering transformations."""

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    dayofweek,
    hour,
    lit,
    month,
    quarter,
    sha2,
    to_date,
    unix_timestamp,
    weekofyear,
    when,
    year,
)


def _ensure_numeric_column(dataframe: DataFrame, column_name: str) -> DataFrame:
    """Ensure numeric column exists for safe downstream feature creation."""
    if column_name in dataframe.columns:
        return dataframe
    return dataframe.withColumn(column_name, lit(None).cast("double"))


def apply_silver_features(
    dataframe: DataFrame,
    pickup_col: str,
    dropoff_col: str,
    distance_col: str,
    fare_col: str,
) -> DataFrame:
    """Add curated row-level silver features (no aggregated KPIs).

    This function intentionally creates only atomic features reusable by
    downstream Gold jobs.
    """
    if pickup_col not in dataframe.columns or dropoff_col not in dataframe.columns:
        return dataframe

    enriched = _ensure_numeric_column(dataframe, distance_col)
    enriched = _ensure_numeric_column(enriched, fare_col)
    for monetary_col in ["tips", "tolls", "airport_fee", "congestion_surcharge", "cbd_congestion_fee"]:
        enriched = _ensure_numeric_column(enriched, monetary_col)

    trip_duration = unix_timestamp(col(dropoff_col)) - unix_timestamp(col(pickup_col))

    enriched = (
        enriched
        .withColumn("trip_date", to_date(col(pickup_col)))
        .withColumn("trip_year", year(col(pickup_col)))
        .withColumn("trip_month_num", month(col(pickup_col)))
        .withColumn("trip_week_of_year", weekofyear(col(pickup_col)))
        .withColumn("trip_quarter", quarter(col(pickup_col)))
        .withColumn("pickup_hour", hour(col(pickup_col)))
        .withColumn("pickup_day_of_week", dayofweek(col(pickup_col)))
        .withColumn("is_weekend", when(col("pickup_day_of_week").isin(1, 7), lit(True)).otherwise(lit(False)))
        .withColumn("time_of_day_bucket",
            when(col("pickup_hour").between(5, 11), lit("morning"))
            .when(col("pickup_hour").between(12, 16), lit("afternoon"))
            .when(col("pickup_hour").between(17, 21), lit("evening"))
            .otherwise(lit("night"))
        )
        .withColumn("trip_duration_sec", trip_duration)
        .withColumn("trip_duration_min", col("trip_duration_sec") / lit(60.0))
        .withColumn(
            "trip_speed_mph",
            when(col("trip_duration_sec") > 0, col(distance_col) / (col("trip_duration_sec") / lit(3600.0)))
            .otherwise(None),
        )
        .withColumn("trip_distance_km", col(distance_col) * lit(1.60934))
        .withColumn(
            "trip_distance_bucket",
            when(col(distance_col) < 1, lit("short"))
            .when(col(distance_col) < 5, lit("medium"))
            .when(col(distance_col) < 15, lit("long"))
            .otherwise(lit("xlong")),
        )
        .withColumn("gross_trip_amount", col(fare_col) + col("tips") + col("tolls") + col("airport_fee") + col("congestion_surcharge") + col("cbd_congestion_fee"))
        .withColumn("tip_pct_of_fare", when(col(fare_col) > 0, col("tips") / col(fare_col)).otherwise(None))
        .withColumn("fare_per_mile", when(col(distance_col) > 0, col(fare_col) / col(distance_col)).otherwise(None))
        .withColumn("fare_per_minute", when(col("trip_duration_min") > 0, col(fare_col) / col("trip_duration_min")).otherwise(None))
        .withColumn("request_to_pickup_sec", when(col("request_datetime").isNotNull(), unix_timestamp(col(pickup_col)) - unix_timestamp(col("request_datetime"))).otherwise(None))
        .withColumn("on_scene_to_pickup_sec", when(col("on_scene_datetime").isNotNull(), unix_timestamp(col(pickup_col)) - unix_timestamp(col("on_scene_datetime"))).otherwise(None))
        .withColumn("pickup_weekday_name", date_format(col(pickup_col), "EEEE"))
    )

    # Stable canonical key for row-level lineage and dedup troubleshooting.
    if "source_file" in enriched.columns:
        enriched = enriched.withColumn(
            "trip_row_key",
            sha2(
                concat_ws(
                    "||",
                    coalesce(col("source_file").cast("string"), lit("")),
                    coalesce(col(pickup_col).cast("string"), lit("")),
                    coalesce(col(dropoff_col).cast("string"), lit("")),
                    coalesce(col(distance_col).cast("string"), lit("")),
                    coalesce(col("PULocationID").cast("string"), lit("")),
                    coalesce(col("DOLocationID").cast("string"), lit("")),
                ),
                256,
            ),
        )

    # Lightweight anomaly flags (still row-level, not KPI aggregations).
    enriched = (
        enriched
        .withColumn("flag_duration_outlier", (col("trip_duration_sec") < 60) | (col("trip_duration_sec") > 14400))
        .withColumn("flag_speed_outlier", (col("trip_speed_mph") > 80) | (col("trip_speed_mph") < 0))
        .withColumn("flag_fare_outlier", (col(fare_col) < 0) | (col(fare_col) > 1000))
    )

    return enriched
