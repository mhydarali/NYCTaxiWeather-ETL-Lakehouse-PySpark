#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG, WeatherConfig
from mobility_lakehouse.pipelines.bronze_weather import ingest_weather_incremental


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch hourly weather data from Open-Meteo API into the bronze layer. Incremental — only fetches dates not yet downloaded.",
        epilog="Example: %(prog)s --start-date 2024-01-01 --end-date 2024-01-07",
    )
    parser.add_argument("--latitude", type=float, default=DEFAULT_CONFIG.weather.latitude,
                        help="Latitude (default: NYC 40.7128)")
    parser.add_argument("--longitude", type=float, default=DEFAULT_CONFIG.weather.longitude,
                        help="Longitude (default: NYC -74.0060)")
    parser.add_argument("--timezone", default=DEFAULT_CONFIG.weather.timezone,
                        help="Timezone for API request (default: America/New_York)")
    parser.add_argument("--start-date", default=DEFAULT_CONFIG.weather.start_date,
                        help="First date to fetch, YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--end-date", default=DEFAULT_CONFIG.weather.end_date,
                        help="Last date to fetch, YYYY-MM-DD (default: 2024-01-03)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = WeatherConfig(
        latitude=args.latitude,
        longitude=args.longitude,
        timezone=args.timezone,
        start_date=args.start_date,
        end_date=args.end_date,
        endpoint=DEFAULT_CONFIG.weather.endpoint,
        hourly_variables=DEFAULT_CONFIG.weather.hourly_variables,
    )
    paths = ingest_weather_incremental(cfg)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
