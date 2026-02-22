# One-Page Cheat Sheet

Quick reference for running and checking the pipeline.

## Inputs

| Source | Type | Typical Size | Landing Path |
|--------|------|--------------|--------------|
| NYC TLC trips | Parquet | ~1 GB per month | `data/bronze/tlc/` |
| Open-Meteo weather | JSON | ~2 KB per day | `data/bronze/weather/` |

## Outputs

| Dataset | Path | Sample Row Count | Partitioning |
|--------|------|------------------|--------------|
| Silver trips | `data/silver/trips/` | 2,927,120 | `pickup_date`, `taxi_type` |
| Silver weather | `data/silver/weather/` | 72 | `date` |
| Silver enriched trips | `data/silver/enriched_trips/` | 2,927,120 | `pickup_date` |
| Gold daily metrics | `data/gold/daily_metrics/` | 35 | `date` |

## Most used commands

```bash
make sync
make test
make lint
make run
.venv/bin/python scripts/inspect_outputs.py --sample-size 3
```

## Custom run command

```bash
.venv/bin/python scripts/run_all.py --mode local \
  --taxi-type yellow --start-month 2024-01 --end-month 2024-01 \
  --weather-start-date 2024-01-01 --weather-end-date 2024-01-02
```

## Stage entrypoints

| Script | Purpose |
|--------|---------|
| `scripts/bronze_tlc.py` | Download monthly TLC parquet files |
| `scripts/bronze_weather.py` | Pull weather API data incrementally |
| `scripts/silver_trips.py` | Clean and standardize trips |
| `scripts/silver_weather.py` | Normalize hourly weather |
| `scripts/silver_enriched.py` | Join trips and weather |
| `scripts/gold_daily_metrics.py` | Build daily aggregates |
| `scripts/run_quality.py` | Run quality checks |
| `scripts/run_all.py` | Run all stages end to end |
| `scripts/inspect_outputs.py` | Show schema, row counts, and samples |

## Join logic

```text
trips.pickup_date = weather.date
AND
trips.pickup_hour = weather.hour
```

- Join type: LEFT JOIN (keeps trips even if weather is missing)
- Timezone: `America/New_York` for both weather API requests and Spark session

## Silver deduplication key

```text
(vendor_id, pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, total_amount)
```

## Quality checks

| Rule | Dataset | Threshold |
|------|---------|-----------|
| Row count > 0 | `silver.trips`, `silver.enriched_trips`, `gold.daily_metrics` | Required |
| `pickup_datetime` not null | `silver.trips` | >= 99.9% |
| `trip_distance >= 0` | `silver.trips` | 100% |
| `fare_amount >= 0` | `silver.trips` | 100% |
| `total_trips` not null | `gold.daily_metrics` | 100% |
| Dedup key uniqueness | `silver.trips` | >= 99.5% |

## Config overrides (`MLH_*`)

Defined in `src/mobility_lakehouse/config.py`.

| Variable | Default |
|----------|---------|
| `MLH_TAXI_TYPE` | `yellow` |
| `MLH_TLC_START_MONTH` | `2024-01` |
| `MLH_TLC_END_MONTH` | `2024-01` |
| `MLH_WEATHER_START_DATE` | `2024-01-01` |
| `MLH_WEATHER_END_DATE` | `2024-01-03` |
| `MLH_SPARK_MASTER` | `local[*]` |
| `MLH_DATA_DIR` | `data` |
