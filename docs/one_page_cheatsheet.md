# One-Page Cheat Sheet

Quick reference for running and understanding the pipeline.

## Inputs

| Source | Type | Size | Location |
|--------|------|------|----------|
| NYC TLC trip data | Parquet (from CDN) | ~1GB per month | `data/bronze/tlc/` |
| Open-Meteo weather | JSON (from API) | ~2KB per day | `data/bronze/weather/` |

## Outputs

| Table | Location | Row Count (1-month run) | Partitioned By |
|-------|----------|------------------------|----------------|
| Cleaned trips | `data/silver/trips/` | ~2.9M | pickup_date, taxi_type |
| Normalized weather | `data/silver/weather/` | 72 (3 days) | date |
| Enriched trips | `data/silver/enriched_trips/` | ~2.9M | pickup_date |
| **Daily metrics (GOLD)** | **`data/gold/daily_metrics/`** | **~35** | **date** |

## Key Commands

```bash
make sync              # Install dependencies
make test              # Run all 6 tests
make lint              # Run ruff linter
make run               # Run full pipeline with defaults (1 month taxi + 3 days weather)

# Custom run
.venv/bin/python scripts/run_all.py --mode local \
  --taxi-type yellow --start-month 2024-01 --end-month 2024-01 \
  --weather-start-date 2024-01-01 --weather-end-date 2024-01-07

# Inspect outputs
.venv/bin/python scripts/inspect_outputs.py

# Run individual stages
.venv/bin/python scripts/bronze_tlc.py --help
.venv/bin/python scripts/silver_trips.py --help
.venv/bin/python scripts/run_quality.py
```

## Core Scripts

| Script | Purpose |
|--------|---------|
| `scripts/run_all.py` | Run entire pipeline end-to-end |
| `scripts/bronze_tlc.py` | Download taxi parquet files |
| `scripts/bronze_weather.py` | Fetch weather from API |
| `scripts/silver_trips.py` | Clean + standardize trips |
| `scripts/silver_weather.py` | Normalize weather data |
| `scripts/silver_enriched.py` | Join trips with weather |
| `scripts/gold_daily_metrics.py` | Aggregate daily metrics |
| `scripts/run_quality.py` | Run quality checks |
| `scripts/inspect_outputs.py` | View output schemas and samples |

## Join Logic

```
trips.pickup_date = weather.date
AND
trips.pickup_hour = weather.hour
```

- Type: LEFT JOIN (trips without weather data are kept, weather columns become null)
- Timezone: Both sources configured to `America/New_York`

## Deduplication Key (Silver Trips)

```
(vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, total_amount)
```

## Data Quality Rules

| Check | Where | Threshold |
|-------|-------|-----------|
| Row count > 0 | silver.trips, silver.enriched, gold.daily | Must not be empty |
| pickup_datetime not null | silver.trips | >= 99.9% |
| total_trips not null | gold.daily_metrics | 100% |
| trip_distance >= 0 | silver.trips | All rows |
| fare_amount >= 0 | silver.trips | All rows |
| avg_fare_amount >= 0 | gold.daily_metrics | All rows |
| Dedup key uniqueness | silver.trips | >= 99.5% |

## Configuration

All settings in `src/mobility_lakehouse/config.py`. Override with `MLH_*` env vars:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MLH_TAXI_TYPE` | yellow | Which taxi type to process |
| `MLH_TLC_START_MONTH` | 2024-01 | First month to download |
| `MLH_TLC_END_MONTH` | 2024-01 | Last month to download |
| `MLH_WEATHER_START_DATE` | 2024-01-01 | First weather date |
| `MLH_WEATHER_END_DATE` | 2024-01-03 | Last weather date |
| `MLH_SPARK_MASTER` | local[*] | Spark master URL |
| `MLH_DATA_DIR` | data | Base directory for all data |
