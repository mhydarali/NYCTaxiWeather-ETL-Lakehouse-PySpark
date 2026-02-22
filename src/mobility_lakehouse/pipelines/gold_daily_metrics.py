from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from mobility_lakehouse.config import GOLD_DIR, SILVER_DIR, SparkConfig
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import mkdirp
from mobility_lakehouse.utils.spark import create_spark_session

logger = setup_logging(__name__)


def build_daily_metrics(enriched_df: DataFrame) -> DataFrame:
    base = enriched_df

    trip_metrics = base.groupBy("pickup_date").agg(
        F.count(F.lit(1)).alias("total_trips"),
        F.avg(F.col("fare_amount")).alias("avg_fare_amount"),
        F.avg(F.col("total_amount")).alias("avg_total_amount"),
        F.avg(F.col("trip_distance")).alias("avg_trip_distance"),
        F.avg(F.expr("total_amount / greatest(trip_distance, 0.1) ")).alias("surge_proxy"),
    )

    payment_map = None
    if "payment_type" in base.columns:
        payment_counts = (
            base.withColumn("payment_type_key", F.col("payment_type").cast("string"))
            .filter(F.col("payment_type_key").isNotNull())
            .groupBy("pickup_date", "payment_type_key")
            .count()
        )
        payment_map = payment_counts.groupBy("pickup_date").agg(
            F.map_from_entries(
                F.collect_list(F.struct(F.col("payment_type_key"), F.col("count")))
            ).alias("payment_type_counts")
        )

    weather_daily = (
        base.select("pickup_date", "pickup_hour", "temperature_2m", "precipitation")
        .dropDuplicates(["pickup_date", "pickup_hour"])
        .groupBy("pickup_date")
        .agg(
            F.avg(F.col("temperature_2m")).alias("avg_temp"),
            F.sum(F.coalesce(F.col("precipitation"), F.lit(0.0))).alias("total_precip"),
        )
    )

    result = trip_metrics.join(weather_daily, on="pickup_date", how="left")
    if payment_map is not None:
        result = result.join(payment_map, on="pickup_date", how="left")
    else:
        result = result.withColumn("payment_type_counts", F.lit(None).cast("map<string,bigint>"))

    return result.withColumnRenamed("pickup_date", "date")


def run_gold_daily_metrics(
    spark_config: SparkConfig,
    silver_enriched_dir: Path = SILVER_DIR / "enriched_trips",
    gold_daily_dir: Path = GOLD_DIR / "daily_metrics",
) -> Path:
    spark = create_spark_session(
        app_name=f"{spark_config.app_name}-gold-daily-metrics",
        master=spark_config.master,
        shuffle_partitions=spark_config.shuffle_partitions,
        timezone=spark_config.timezone,
    )
    try:
        enriched = spark.read.parquet(str(silver_enriched_dir))
        metrics = build_daily_metrics(enriched)
        mkdirp(gold_daily_dir)
        metrics.write.mode("overwrite").partitionBy("date").parquet(str(gold_daily_dir))
        logger.info("gold daily metrics written", extra={"output": str(gold_daily_dir)})
        return gold_daily_dir
    finally:
        spark.stop()
