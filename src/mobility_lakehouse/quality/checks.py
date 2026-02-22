from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from mobility_lakehouse.config import GOLD_DIR, SILVER_DIR


def row_count_nonzero(df: DataFrame, table_name: str) -> list[str]:
    count = df.count()
    if count <= 0:
        return [f"{table_name}: row count is zero"]
    return []


def column_not_null_rate(df: DataFrame, column: str, min_rate: float, table_name: str) -> list[str]:
    if column not in df.columns:
        return [f"{table_name}: missing required column '{column}'"]
    total = df.count()
    if total == 0:
        return [f"{table_name}: cannot evaluate null-rate on empty table"]
    non_null = df.filter(F.col(column).isNotNull()).count()
    rate = non_null / total
    if rate < min_rate:
        return [f"{table_name}: column '{column}' non-null rate {rate:.3f} < {min_rate:.3f}"]
    return []


def value_ranges(
    df: DataFrame,
    column: str,
    min_value: float | None,
    max_value: float | None,
    table_name: str,
) -> list[str]:
    if column not in df.columns:
        return [f"{table_name}: missing required column '{column}'"]

    stats = df.agg(F.min(F.col(column)).alias("min_val"), F.max(F.col(column)).alias("max_val")).first()
    min_val = stats["min_val"]
    max_val = stats["max_val"]

    problems: list[str] = []
    if min_value is not None and min_val is not None and float(min_val) < float(min_value):
        problems.append(f"{table_name}: column '{column}' min {min_val} < {min_value}")
    if max_value is not None and max_val is not None and float(max_val) > float(max_value):
        problems.append(f"{table_name}: column '{column}' max {max_val} > {max_value}")
    return problems


def uniqueness(df: DataFrame, key_columns: list[str], min_ratio: float, table_name: str) -> list[str]:
    missing = [col for col in key_columns if col not in df.columns]
    if missing:
        return [f"{table_name}: missing uniqueness key columns {missing}"]

    total = df.count()
    if total == 0:
        return [f"{table_name}: cannot evaluate uniqueness on empty table"]

    unique_count = df.select(*key_columns).distinct().count()
    ratio = unique_count / total
    if ratio < min_ratio:
        return [f"{table_name}: uniqueness ratio {ratio:.3f} < {min_ratio:.3f} for {key_columns}"]
    return []


def _read_if_exists(spark: SparkSession, path: Path) -> DataFrame | None:
    return spark.read.parquet(str(path)) if path.exists() else None


def run_quality_checks(
    spark: SparkSession,
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> list[str]:
    issues: list[str] = []

    trips = _read_if_exists(spark, silver_dir / "trips")
    enriched = _read_if_exists(spark, silver_dir / "enriched_trips")
    daily = _read_if_exists(spark, gold_dir / "daily_metrics")

    if trips is None:
        issues.append("silver.trips: missing table")
    else:
        issues.extend(row_count_nonzero(trips, "silver.trips"))
        issues.extend(column_not_null_rate(trips, "pickup_datetime", 0.999, "silver.trips"))
        issues.extend(value_ranges(trips, "trip_distance", 0.0, None, "silver.trips"))
        issues.extend(value_ranges(trips, "fare_amount", 0.0, None, "silver.trips"))
        issues.extend(
            uniqueness(
                trips,
                [
                    "vendor_id",
                    "pickup_datetime",
                    "dropoff_datetime",
                    "pu_location_id",
                    "do_location_id",
                    "total_amount",
                ],
                0.995,
                "silver.trips",
            )
        )

    if enriched is None:
        issues.append("silver.enriched_trips: missing table")
    else:
        issues.extend(row_count_nonzero(enriched, "silver.enriched_trips"))

    if daily is None:
        issues.append("gold.daily_metrics: missing table")
    else:
        issues.extend(row_count_nonzero(daily, "gold.daily_metrics"))
        issues.extend(column_not_null_rate(daily, "total_trips", 1.0, "gold.daily_metrics"))
        issues.extend(value_ranges(daily, "avg_fare_amount", 0.0, None, "gold.daily_metrics"))

    return issues
