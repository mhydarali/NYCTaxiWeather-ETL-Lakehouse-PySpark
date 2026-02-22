from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
import os

DATA_DIR = Path(os.getenv("MLH_DATA_DIR", "data"))
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"
LOG_DIR = Path(os.getenv("MLH_LOG_DIR", "logs"))

DEFAULT_TLC_URL_PATTERN = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "{taxi_type}_tripdata_{year}-{month:02d}.parquet"
)
DEFAULT_WEATHER_ENDPOINT = "https://archive-api.open-meteo.com/v1/archive"


@dataclass(frozen=True)
class TLCConfig:
    taxi_type: str = os.getenv("MLH_TAXI_TYPE", "yellow")
    start_month: str = os.getenv("MLH_TLC_START_MONTH", "2024-01")
    end_month: str = os.getenv("MLH_TLC_END_MONTH", "2024-01")
    base_url_pattern: str = os.getenv("MLH_TLC_URL_PATTERN", DEFAULT_TLC_URL_PATTERN)


@dataclass(frozen=True)
class WeatherConfig:
    latitude: float = float(os.getenv("MLH_WEATHER_LAT", "40.7128"))
    longitude: float = float(os.getenv("MLH_WEATHER_LON", "-74.0060"))
    timezone: str = os.getenv("MLH_WEATHER_TZ", "America/New_York")
    start_date: str = os.getenv("MLH_WEATHER_START_DATE", "2024-01-01")
    end_date: str = os.getenv("MLH_WEATHER_END_DATE", "2024-01-03")
    endpoint: str = os.getenv("MLH_WEATHER_ENDPOINT", DEFAULT_WEATHER_ENDPOINT)
    hourly_variables: list[str] = field(
        default_factory=lambda: ["temperature_2m", "precipitation", "wind_speed_10m"]
    )


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = os.getenv("MLH_SPARK_APP_NAME", "mobility-lakehouse")
    master: str = os.getenv("MLH_SPARK_MASTER", "local[*]")
    shuffle_partitions: int = int(os.getenv("MLH_SPARK_SHUFFLE_PARTITIONS", "8"))
    timezone: str = os.getenv("MLH_SPARK_TIMEZONE", "America/New_York")


@dataclass(frozen=True)
class AppConfig:
    data_dir: Path = DATA_DIR
    bronze_dir: Path = BRONZE_DIR
    silver_dir: Path = SILVER_DIR
    gold_dir: Path = GOLD_DIR
    log_dir: Path = LOG_DIR
    tlc: TLCConfig = field(default_factory=TLCConfig)
    weather: WeatherConfig = field(default_factory=WeatherConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)


def parse_month(month_str: str) -> date:
    return datetime.strptime(month_str, "%Y-%m").date().replace(day=1)


def month_range(start_month: str, end_month: str) -> list[tuple[int, int]]:
    start = parse_month(start_month)
    end = parse_month(end_month)
    if start > end:
        raise ValueError(f"start_month {start_month} cannot be after end_month {end_month}")

    months: list[tuple[int, int]] = []
    year, month = start.year, start.month
    while (year, month) <= (end.year, end.month):
        months.append((year, month))
        if month == 12:
            year += 1
            month = 1
        else:
            month += 1
    return months


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


DEFAULT_CONFIG = AppConfig()
