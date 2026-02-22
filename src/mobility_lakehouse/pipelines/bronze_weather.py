from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
import json

import requests

from mobility_lakehouse.config import BRONZE_DIR, WeatherConfig, parse_iso_date
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.paths import atomic_write_json, atomic_write_text, mkdirp, read_json

logger = setup_logging(__name__)


# This file tracks the last date we successfully fetched weather for.
# On re-runs, the pipeline reads this file and only fetches dates after it.
# This makes the pipeline "incremental" — it won't re-download old data.
STATE_FILE_NAME = "_state.json"


def _date_iter(start: date, end: date) -> list[date]:
    out: list[date] = []
    current = start
    while current <= end:
        out.append(current)
        current += timedelta(days=1)
    return out


def load_state(state_path: Path) -> dict:
    return read_json(state_path, default={}) or {}


def save_state(state_path: Path, last_ingested_date: date) -> Path:
    payload = {"last_ingested_date": last_ingested_date.isoformat()}
    return atomic_write_json(state_path, payload)


def get_dates_to_fetch(
    weather_config: WeatherConfig,
    state_path: Path,
    today: date | None = None,
) -> list[date]:
    today = today or date.today()
    configured_start = parse_iso_date(weather_config.start_date)
    configured_end = min(parse_iso_date(weather_config.end_date), today)

    state = load_state(state_path)
    last_ingested = state.get("last_ingested_date")

    if last_ingested:
        next_date = parse_iso_date(last_ingested) + timedelta(days=1)
        start = max(configured_start, next_date)
    else:
        start = configured_start

    if start > configured_end:
        return []

    return _date_iter(start, configured_end)


def fetch_weather_for_day(
    weather_config: WeatherConfig,
    target_date: date,
    session: requests.Session | None = None,
    timeout: int = 60,
) -> dict:
    http = session or requests.Session()
    params = {
        "latitude": weather_config.latitude,
        "longitude": weather_config.longitude,
        "start_date": target_date.isoformat(),
        "end_date": target_date.isoformat(),
        "hourly": ",".join(weather_config.hourly_variables),
        "timezone": weather_config.timezone,
    }
    response = http.get(weather_config.endpoint, params=params, timeout=timeout)
    response.raise_for_status()
    payload = response.json()
    payload["_metadata"] = {
        "fetched_for_date": target_date.isoformat(),
        "endpoint": weather_config.endpoint,
    }
    return payload


def ingest_weather_incremental(
    weather_config: WeatherConfig,
    bronze_weather_dir: Path = BRONZE_DIR / "weather",
    session: requests.Session | None = None,
    today: date | None = None,
) -> list[Path]:
    """Fetch weather data day by day, skipping dates already downloaded.

    Uses a checkpoint file (_state.json) to remember progress, so the
    pipeline is safe to interrupt and re-run.
    """
    mkdirp(bronze_weather_dir)
    state_path = bronze_weather_dir / STATE_FILE_NAME
    dates = get_dates_to_fetch(weather_config, state_path=state_path, today=today)

    outputs: list[Path] = []
    for target_date in dates:
        output_path = bronze_weather_dir / f"date={target_date.isoformat()}" / "weather.json"
        if output_path.exists() and output_path.stat().st_size > 0:
            logger.info("skip existing weather file", extra={"path": str(output_path)})
            save_state(state_path, target_date)
            outputs.append(output_path)
            continue

        payload = fetch_weather_for_day(weather_config, target_date=target_date, session=session)
        mkdirp(output_path.parent)
        atomic_write_text(output_path, json.dumps(payload, indent=2, sort_keys=True))
        save_state(state_path, target_date)
        outputs.append(output_path)

    return outputs
