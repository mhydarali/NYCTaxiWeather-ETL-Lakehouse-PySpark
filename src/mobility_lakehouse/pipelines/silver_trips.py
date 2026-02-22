from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from mobility_lakehouse.config import BRONZE_DIR, SILVER_DIR, SparkConfig
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import mkdirp
from mobility_lakehouse.utils.spark import create_spark_session

logger = setup_logging(__name__)


def _col_lookup(df: DataFrame) -> dict[str, str]:
    """Build a lowercase→original column name mapping for case-insensitive lookups."""
    return {col.lower(): col for col in df.columns}


def _find_col(df: DataFrame, *candidates: str):
    """Find a column by trying multiple possible names (case-insensitive).

    Why: Yellow taxis use 'tpep_pickup_datetime', green taxis use
    'lpep_pickup_datetime', and casing varies ('PULocationID' vs 'pulocationid').
    This lets us write one cleaning function that handles all taxi types.
    """
    lookup = _col_lookup(df)
    for candidate in candidates:
        resolved = lookup.get(candidate.lower())
        if resolved is not None:
            return F.col(resolved)
    # Column not found in this taxi type — return null so downstream logic still works
    return F.lit(None)


def clean_trips(df: DataFrame, taxi_type: str) -> DataFrame:
    standardized = df.select(
        _find_col(df, "vendorid", "vendor_id").cast("int").alias("vendor_id"),
        F.to_timestamp(
            _find_col(df, "tpep_pickup_datetime", "lpep_pickup_datetime", "pickup_datetime")
        ).alias("pickup_datetime"),
        F.to_timestamp(
            _find_col(df, "tpep_dropoff_datetime", "lpep_dropoff_datetime", "dropoff_datetime")
        ).alias("dropoff_datetime"),
        _find_col(df, "pulocationid", "pu_location_id").cast("int").alias("pu_location_id"),
        _find_col(df, "dolocationid", "do_location_id").cast("int").alias("do_location_id"),
        _find_col(df, "passenger_count").cast("double").alias("passenger_count"),
        _find_col(df, "trip_distance").cast("double").alias("trip_distance"),
        _find_col(df, "fare_amount").cast("double").alias("fare_amount"),
        _find_col(df, "total_amount").cast("double").alias("total_amount"),
        _find_col(df, "payment_type").cast("int").alias("payment_type"),
    ).withColumn("taxi_type", F.lit(taxi_type))

    cleaned = (
        standardized.filter(F.col("pickup_datetime").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .filter(F.col("dropoff_datetime") >= F.col("pickup_datetime"))
        .filter(F.col("trip_distance") >= 0)
        .filter(F.col("fare_amount") >= 0)
        .dropDuplicates(
            [
                "vendor_id",
                "pickup_datetime",
                "dropoff_datetime",
                "pu_location_id",
                "do_location_id",
                "total_amount",
            ]
        )
        .withColumn("pickup_date", F.to_date("pickup_datetime"))
        .withColumn("pickup_hour", F.hour("pickup_datetime"))
    )
    return cleaned


def _list_available_taxi_types(bronze_tlc_dir: Path) -> list[str]:
    if not bronze_tlc_dir.exists():
        return []
    values: list[str] = []
    for path in bronze_tlc_dir.glob("taxi_type=*"):
        if path.is_dir() and "=" in path.name:
            values.append(path.name.split("=", 1)[1])
    return sorted(values)


def run_silver_trips(
    spark: SparkSession,
    bronze_tlc_dir: Path = BRONZE_DIR / "tlc",
    silver_trips_dir: Path = SILVER_DIR / "trips",
    taxi_types: list[str] | None = None,
) -> Path:
    taxi_types = taxi_types or _list_available_taxi_types(bronze_tlc_dir)
    if not taxi_types:
        raise FileNotFoundError(f"No TLC taxi types found under {bronze_tlc_dir}")

    frames: list[DataFrame] = []
    for taxi_type in taxi_types:
        source = bronze_tlc_dir / f"taxi_type={taxi_type}"
        if not source.exists():
            continue
        raw = spark.read.parquet(str(source))
        frames.append(clean_trips(raw, taxi_type=taxi_type))

    if not frames:
        raise FileNotFoundError(f"No TLC parquet files found under {bronze_tlc_dir}")

    unioned = frames[0]
    for frame in frames[1:]:
        unioned = unioned.unionByName(frame, allowMissingColumns=True)

    mkdirp(silver_trips_dir)
    (
        unioned.write.mode("overwrite")
        .partitionBy("pickup_date", "taxi_type")
        .parquet(str(silver_trips_dir))
    )
    logger.info("silver trips written", extra={"output": str(silver_trips_dir)})
    return silver_trips_dir


def run_job(
    spark_config: SparkConfig,
    bronze_tlc_dir: Path = BRONZE_DIR / "tlc",
    silver_trips_dir: Path = SILVER_DIR / "trips",
    taxi_types: list[str] | None = None,
) -> Path:
    spark = create_spark_session(
        app_name=f"{spark_config.app_name}-silver-trips",
        master=spark_config.master,
        shuffle_partitions=spark_config.shuffle_partitions,
        timezone=spark_config.timezone,
    )
    try:
        return run_silver_trips(
            spark=spark,
            bronze_tlc_dir=bronze_tlc_dir,
            silver_trips_dir=silver_trips_dir,
            taxi_types=taxi_types,
        )
    finally:
        spark.stop()
