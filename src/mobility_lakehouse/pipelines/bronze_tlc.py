from __future__ import annotations

from pathlib import Path
from typing import Iterable

import requests

from mobility_lakehouse.config import BRONZE_DIR, TLCConfig, month_range
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import mkdirp

logger = setup_logging(__name__)


def build_tlc_url(taxi_type: str, year: int, month: int, base_url_pattern: str) -> str:
    return base_url_pattern.format(taxi_type=taxi_type, year=year, month=month)


def _destination_path(dest_dir: Path, taxi_type: str, year: int, month: int) -> Path:
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    return dest_dir / f"taxi_type={taxi_type}" / f"year={year:04d}" / f"month={month:02d}" / filename


def download_tlc_month(
    taxi_type: str,
    year: int,
    month: int,
    dest_dir: Path,
    base_url_pattern: str,
    session: requests.Session | None = None,
    timeout: int = 120,
) -> Path:
    destination = _destination_path(dest_dir, taxi_type, year, month)
    if destination.exists() and destination.stat().st_size > 0:
        logger.info("skip existing tlc file", extra={"path": str(destination)})
        return destination

    mkdirp(destination.parent)
    url = build_tlc_url(taxi_type, year, month, base_url_pattern)
    http = session or requests.Session()
    logger.info("downloading tlc parquet", extra={"url": url})

    response = http.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    tmp_path = destination.with_suffix(destination.suffix + ".part")
    with tmp_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)

    if tmp_path.stat().st_size == 0:
        tmp_path.unlink(missing_ok=True)
        raise RuntimeError(f"Downloaded empty file for {url}")

    tmp_path.replace(destination)
    return destination


def download_tlc_range(
    tlc_config: TLCConfig,
    dest_dir: Path = BRONZE_DIR / "tlc",
    session: requests.Session | None = None,
) -> list[Path]:
    outputs: list[Path] = []
    months: Iterable[tuple[int, int]] = month_range(tlc_config.start_month, tlc_config.end_month)
    for year, month in months:
        outputs.append(
            download_tlc_month(
                taxi_type=tlc_config.taxi_type,
                year=year,
                month=month,
                dest_dir=dest_dir,
                base_url_pattern=tlc_config.base_url_pattern,
                session=session,
            )
        )
    return outputs
