#!/usr/bin/env python
from __future__ import annotations

import argparse
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from mobility_lakehouse.config import DEFAULT_CONFIG, TLCConfig
from mobility_lakehouse.pipelines.bronze_tlc import download_tlc_range


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download NYC TLC taxi trip parquet files into the bronze layer.",
        epilog="Example: %(prog)s --taxi-type yellow --start-month 2024-01 --end-month 2024-03",
    )
    parser.add_argument("--taxi-type", default=DEFAULT_CONFIG.tlc.taxi_type, choices=["yellow", "green"],
                        help="Which taxi type to download (default: yellow)")
    parser.add_argument("--start-month", default=DEFAULT_CONFIG.tlc.start_month,
                        help="First month to download, YYYY-MM (default: 2024-01)")
    parser.add_argument("--end-month", default=DEFAULT_CONFIG.tlc.end_month,
                        help="Last month to download, YYYY-MM (default: 2024-01)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = TLCConfig(
        taxi_type=args.taxi_type,
        start_month=args.start_month,
        end_month=args.end_month,
        base_url_pattern=DEFAULT_CONFIG.tlc.base_url_pattern,
    )
    paths = download_tlc_range(cfg)
    for path in paths:
        print(path)


if __name__ == "__main__":
    main()
