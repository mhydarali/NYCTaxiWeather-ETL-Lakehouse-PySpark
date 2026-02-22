#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG, SparkConfig
from mobility_lakehouse.pipelines.silver_weather import run_silver_weather


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize bronze weather JSON into silver parquet. Parses timestamps, casts types, and partitions by date.",
        epilog="Example: %(prog)s",
    )
    parser.add_argument("--master", default=DEFAULT_CONFIG.spark.master,
                        help="Spark master URL (default: local[*])")
    parser.add_argument("--shuffle-partitions", type=int, default=DEFAULT_CONFIG.spark.shuffle_partitions,
                        help="Number of Spark shuffle partitions (default: 8)")
    parser.add_argument("--timezone", default=DEFAULT_CONFIG.spark.timezone,
                        help="Spark session timezone (default: America/New_York)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark_cfg = SparkConfig(
        app_name=DEFAULT_CONFIG.spark.app_name,
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        timezone=args.timezone,
    )
    output = run_silver_weather(spark_cfg)
    print(output)


if __name__ == "__main__":
    main()
