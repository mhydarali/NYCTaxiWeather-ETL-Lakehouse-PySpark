#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG, SparkConfig
from mobility_lakehouse.pipelines.silver_trips import run_job


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and standardize bronze taxi trip data into silver parquet. Handles schema differences between yellow/green taxis, removes invalid rows, and deduplicates.",
        epilog="Example: %(prog)s --taxi-types yellow,green",
    )
    parser.add_argument("--master", default=DEFAULT_CONFIG.spark.master,
                        help="Spark master URL (default: local[*])")
    parser.add_argument("--shuffle-partitions", type=int, default=DEFAULT_CONFIG.spark.shuffle_partitions,
                        help="Number of Spark shuffle partitions (default: 8)")
    parser.add_argument("--timezone", default=DEFAULT_CONFIG.spark.timezone,
                        help="Spark session timezone (default: America/New_York)")
    parser.add_argument("--taxi-types", default="",
                        help="Comma-separated taxi types to process, e.g. yellow,green (default: all available)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    taxi_types = [part.strip() for part in args.taxi_types.split(",") if part.strip()] or None
    spark_cfg = SparkConfig(
        app_name=DEFAULT_CONFIG.spark.app_name,
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        timezone=args.timezone,
    )
    output = run_job(spark_cfg, taxi_types=taxi_types)
    print(output)


if __name__ == "__main__":
    main()
