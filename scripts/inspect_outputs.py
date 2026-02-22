#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG
from mobility_lakehouse.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Show schemas, row counts, partitions, and sample rows for all silver and gold tables. Useful for verifying pipeline outputs.",
        epilog="Example: %(prog)s --sample-size 3",
    )
    parser.add_argument("--master", default=DEFAULT_CONFIG.spark.master,
                        help="Spark master URL (default: local[*])")
    parser.add_argument("--shuffle-partitions", type=int, default=DEFAULT_CONFIG.spark.shuffle_partitions,
                        help="Number of Spark shuffle partitions (default: 8)")
    parser.add_argument("--timezone", default=DEFAULT_CONFIG.spark.timezone,
                        help="Spark session timezone (default: America/New_York)")
    parser.add_argument("--sample-size", type=int, default=5,
                        help="Number of sample rows to display per table (default: 5)")
    return parser.parse_args()


def list_partitions(path: Path) -> list[str]:
    if not path.exists():
        return []
    return sorted(p.name for p in path.iterdir() if p.is_dir() and "=" in p.name)


def inspect_table(spark, label: str, path: Path, sample_size: int) -> None:
    print(f"\\n== {label} ==")
    if not path.exists():
        print(f"missing: {path}")
        return
    partitions = list_partitions(path)
    print(f"path: {path}")
    print(f"partitions: {partitions[:20]}")
    df = spark.read.parquet(str(path))
    print(f"row_count: {df.count()}")
    print("schema:")
    df.printSchema()
    print(f"sample ({sample_size} rows):")
    df.show(sample_size, truncate=False)


def main() -> None:
    args = parse_args()
    spark = create_spark_session(
        app_name=f"{DEFAULT_CONFIG.spark.app_name}-inspect",
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        timezone=args.timezone,
    )
    try:
        inspect_table(spark, "silver.trips", DEFAULT_CONFIG.silver_dir / "trips", args.sample_size)
        inspect_table(spark, "silver.weather", DEFAULT_CONFIG.silver_dir / "weather", args.sample_size)
        inspect_table(
            spark,
            "silver.enriched_trips",
            DEFAULT_CONFIG.silver_dir / "enriched_trips",
            args.sample_size,
        )
        inspect_table(spark, "gold.daily_metrics", DEFAULT_CONFIG.gold_dir / "daily_metrics", args.sample_size)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
