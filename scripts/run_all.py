#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG
from mobility_lakehouse.pipelines.run_all import PIPELINE_STEPS, run_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the full city mobility lakehouse pipeline end-to-end.",
        epilog="Example: %(prog)s --mode local --taxi-type yellow --start-month 2024-01 --end-month 2024-01",
    )
    parser.add_argument("--mode", choices=["local", "docker"], default="local",
                        help="Run Spark locally or submit to a Docker cluster (default: local)")
    parser.add_argument("--skip-steps", default="",
                        help=f"Comma-separated stages to skip. Options: {','.join(PIPELINE_STEPS)}")

    parser.add_argument("--taxi-type", default=DEFAULT_CONFIG.tlc.taxi_type, choices=["yellow", "green"],
                        help="Which taxi type to process (default: yellow)")
    parser.add_argument("--start-month", default=DEFAULT_CONFIG.tlc.start_month,
                        help="First month to download, format YYYY-MM (default: 2024-01)")
    parser.add_argument("--end-month", default=DEFAULT_CONFIG.tlc.end_month,
                        help="Last month to download, format YYYY-MM (default: 2024-01)")

    parser.add_argument("--weather-lat", type=float, default=DEFAULT_CONFIG.weather.latitude,
                        help="Latitude for weather data (default: NYC 40.7128)")
    parser.add_argument("--weather-lon", type=float, default=DEFAULT_CONFIG.weather.longitude,
                        help="Longitude for weather data (default: NYC -74.0060)")
    parser.add_argument("--weather-timezone", default=DEFAULT_CONFIG.weather.timezone,
                        help="Timezone for weather API requests (default: America/New_York)")
    parser.add_argument("--weather-start-date", default=DEFAULT_CONFIG.weather.start_date,
                        help="First date for weather data, YYYY-MM-DD (default: 2024-01-01)")
    parser.add_argument("--weather-end-date", default=DEFAULT_CONFIG.weather.end_date,
                        help="Last date for weather data, YYYY-MM-DD (default: 2024-01-03)")
    return parser.parse_args()


def main() -> None:
    run_pipeline(parse_args())


if __name__ == "__main__":
    main()
