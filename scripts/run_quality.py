#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG
from mobility_lakehouse.quality.checks import run_quality_checks
from mobility_lakehouse.utils.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run automated data quality checks on silver and gold tables. Validates row counts, null rates, value ranges, and uniqueness. Exits with code 1 if any check fails.",
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
    spark = create_spark_session(
        app_name=f"{DEFAULT_CONFIG.spark.app_name}-quality",
        master=args.master,
        shuffle_partitions=args.shuffle_partitions,
        timezone=args.timezone,
    )
    try:
        issues = run_quality_checks(spark)
    finally:
        spark.stop()

    if issues:
        for issue in issues:
            print(f"FAIL: {issue}")
        raise SystemExit(1)

    print("All quality checks passed")


if __name__ == "__main__":
    main()
