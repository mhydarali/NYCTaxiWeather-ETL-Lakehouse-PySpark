from __future__ import annotations

from pathlib import Path
import json

import pyspark.sql.functions as F

from mobility_lakehouse.config import BRONZE_DIR, SILVER_DIR, SparkConfig
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import mkdirp
from mobility_lakehouse.utils.spark import create_spark_session

logger = setup_logging(__name__)


def _safe_at(values: list | None, index: int):
    if values is None or index >= len(values):
        return None
    return values[index]


def extract_weather_rows(bronze_weather_dir: Path) -> list[dict]:
    rows: list[dict] = []
    for weather_file in sorted(bronze_weather_dir.glob("date=*/weather.json")):
        payload = json.loads(weather_file.read_text(encoding="utf-8"))
        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        temperatures = hourly.get("temperature_2m")
        precipitation = hourly.get("precipitation")
        wind = hourly.get("wind_speed_10m") or hourly.get("windspeed_10m")

        for idx, timestamp in enumerate(times):
            rows.append(
                {
                    "timestamp": timestamp,
                    "temperature_2m": _safe_at(temperatures, idx),
                    "precipitation": _safe_at(precipitation, idx),
                    "windspeed_10m": _safe_at(wind, idx),
                }
            )
    return rows


def run_silver_weather(
    spark_config: SparkConfig,
    bronze_weather_dir: Path = BRONZE_DIR / "weather",
    silver_weather_dir: Path = SILVER_DIR / "weather",
) -> Path:
    rows = extract_weather_rows(bronze_weather_dir)
    if not rows:
        raise FileNotFoundError(f"No weather JSON files found under {bronze_weather_dir}")

    spark = create_spark_session(
        app_name=f"{spark_config.app_name}-silver-weather",
        master=spark_config.master,
        shuffle_partitions=spark_config.shuffle_partitions,
        timezone=spark_config.timezone,
    )
    try:
        df = spark.createDataFrame(rows)
        timestamp_col = F.coalesce(
            F.to_timestamp(F.col("timestamp")),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm"),
            F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"),
        )
        normalized = (
            df.withColumn("timestamp", timestamp_col)
            .withColumn("temperature_2m", F.col("temperature_2m").cast("double"))
            .withColumn("precipitation", F.col("precipitation").cast("double"))
            .withColumn("windspeed_10m", F.col("windspeed_10m").cast("double"))
            .withColumn("date", F.to_date("timestamp"))
            .withColumn("hour", F.hour("timestamp"))
            .filter(F.col("timestamp").isNotNull())
        )

        mkdirp(silver_weather_dir)
        normalized.write.mode("overwrite").partitionBy("date").parquet(str(silver_weather_dir))
        logger.info("silver weather written", extra={"output": str(silver_weather_dir)})
        return silver_weather_dir
    finally:
        spark.stop()
