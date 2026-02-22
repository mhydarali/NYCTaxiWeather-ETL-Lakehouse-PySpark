from __future__ import annotations

from datetime import date
from pathlib import Path

from mobility_lakehouse.config import WeatherConfig
from mobility_lakehouse.pipelines.bronze_weather import (
    get_dates_to_fetch,
    ingest_weather_incremental,
    save_state,
)


class _FakeResponse:
    def __init__(self, payload: dict):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: int):
        self.calls.append((url, params))
        start_date = params["start_date"]
        payload = {
            "hourly": {
                "time": [f"{start_date}T00:00", f"{start_date}T01:00"],
                "temperature_2m": [1.0, 2.0],
                "precipitation": [0.0, 0.1],
                "wind_speed_10m": [3.0, 4.0],
            }
        }
        return _FakeResponse(payload)


def test_get_dates_to_fetch_respects_state(tmp_path: Path):
    cfg = WeatherConfig(
        latitude=40.7,
        longitude=-74.0,
        timezone="America/New_York",
        start_date="2024-01-01",
        end_date="2024-01-05",
        endpoint="https://archive-api.open-meteo.com/v1/archive",
        hourly_variables=["temperature_2m"],
    )
    state_path = tmp_path / "_state.json"

    dates = get_dates_to_fetch(cfg, state_path=state_path, today=date(2024, 1, 10))
    assert [d.isoformat() for d in dates] == [
        "2024-01-01",
        "2024-01-02",
        "2024-01-03",
        "2024-01-04",
        "2024-01-05",
    ]

    save_state(state_path, date(2024, 1, 3))
    dates = get_dates_to_fetch(cfg, state_path=state_path, today=date(2024, 1, 10))
    assert [d.isoformat() for d in dates] == ["2024-01-04", "2024-01-05"]


def test_ingest_weather_incremental_uses_state_and_writes_files(tmp_path: Path):
    bronze_dir = tmp_path / "weather"
    cfg = WeatherConfig(
        latitude=40.7,
        longitude=-74.0,
        timezone="America/New_York",
        start_date="2024-01-01",
        end_date="2024-01-02",
        endpoint="https://archive-api.open-meteo.com/v1/archive",
        hourly_variables=["temperature_2m", "precipitation", "wind_speed_10m"],
    )
    session = _FakeSession()

    outputs = ingest_weather_incremental(
        weather_config=cfg,
        bronze_weather_dir=bronze_dir,
        session=session,
        today=date(2024, 1, 3),
    )

    assert len(outputs) == 2
    assert len(session.calls) == 2
    assert (bronze_dir / "_state.json").exists()

    outputs_2 = ingest_weather_incremental(
        weather_config=cfg,
        bronze_weather_dir=bronze_dir,
        session=session,
        today=date(2024, 1, 3),
    )

    assert outputs_2 == []
    assert len(session.calls) == 2
