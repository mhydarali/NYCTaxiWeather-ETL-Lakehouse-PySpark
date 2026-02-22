from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from mobility_lakehouse.config import SILVER_DIR, SparkConfig
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import mkdirp
from mobility_lakehouse.utils.spark import create_spark_session

logger = setup_logging(__name__)


def join_trips_weather(trips_df: DataFrame, weather_df: DataFrame) -> DataFrame:
    trips = (
        trips_df.withColumn("pickup_date", F.to_date("pickup_date"))
        .withColumn("pickup_hour", F.col("pickup_hour").cast("int"))
    )
    weather = weather_df.select(
        F.to_date("date").alias("weather_date"),
        F.col("hour").cast("int").alias("weather_hour"),
        F.col("temperature_2m"),
        F.col("precipitation"),
        F.col("windspeed_10m"),
    )

    return (
        trips.join(
            weather,
            (trips.pickup_date == weather.weather_date)
            & (trips.pickup_hour == weather.weather_hour),
            how="left",
        )
        .drop("weather_date", "weather_hour")
    )


def run_silver_enriched(
    spark_config: SparkConfig,
    silver_trips_dir: Path = SILVER_DIR / "trips",
    silver_weather_dir: Path = SILVER_DIR / "weather",
    silver_enriched_dir: Path = SILVER_DIR / "enriched_trips",
) -> Path:
    spark = create_spark_session(
        app_name=f"{spark_config.app_name}-silver-enriched",
        master=spark_config.master,
        shuffle_partitions=spark_config.shuffle_partitions,
        timezone=spark_config.timezone,
    )
    try:
        trips = spark.read.parquet(str(silver_trips_dir))
        weather = spark.read.parquet(str(silver_weather_dir))
        enriched = join_trips_weather(trips, weather)

        mkdirp(silver_enriched_dir)
        enriched.write.mode("overwrite").partitionBy("pickup_date").parquet(str(silver_enriched_dir))
        logger.info("silver enriched trips written", extra={"output": str(silver_enriched_dir)})
        return silver_enriched_dir
    finally:
        spark.stop()
