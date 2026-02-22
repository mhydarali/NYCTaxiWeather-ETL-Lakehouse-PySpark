# Run Evidence (Personal Project #2)

This file summarizes reproducible validation and output checks from the local sample run.

## Validation Commands

```bash
make sync
make test
make lint
.venv/bin/python scripts/run_all.py --mode local --taxi-type yellow --start-month 2024-01 --end-month 2024-01 --weather-start-date 2024-01-01 --weather-end-date 2024-01-02
.venv/bin/python scripts/inspect_outputs.py --sample-size 1
```

## Validation Status
- `make sync`: PASS
- `make test`: PASS (6 passed)
- `make lint`: PASS
- End-to-end sample pipeline run: PASS (`pipeline completed`)

## Table Row Counts (from `inspect_outputs.py`)
- `silver.trips`: `2,927,120`
- `silver.weather`: `48`
- `silver.enriched_trips`: `2,927,120`
- `gold.daily_metrics`: `35`

## Output Partitions Found

### `data/silver/trips`
Example partition directories observed:
- `pickup_date=2024-01-23/taxi_type=yellow`
- `pickup_date=2024-01-24/taxi_type=yellow`
- `pickup_date=2024-01-12/taxi_type=yellow`

### `data/silver/weather`
- `date=2024-01-01`
- `date=2024-01-02`

### `data/silver/enriched_trips`
Example partition directories observed:
- `pickup_date=2024-01-23`
- `pickup_date=2024-01-24`
- `pickup_date=2024-01-12`

### `data/gold/daily_metrics`
Example partition directories observed:
- `date=2024-01-02`
- `date=2024-01-05`
- `date=2024-01-04`
- `date=2024-01-03`

## Gold Sample Output Columns
From schema inspection:
- `date`
- `total_trips`
- `avg_fare_amount`
- `avg_total_amount`
- `avg_trip_distance`
- `surge_proxy`
- `payment_type_counts`
- `avg_temp`
- `total_precip`

## Known Warnings (Non-Fatal)
### Spark MemoryManager row-group warnings
During Parquet writes, Spark may log messages like:
- `Total allocation exceeds 95% ... Scaling row group sizes ...`

Interpretation:
- Spark is reducing Parquet row-group size to stay within memory limits.
- This is expected under heavy write pressure and did not cause job failure in this run.

### RowBasedKeyValueBatch spill warnings
Spark may log `Calling spill() ... Will not spill but return 0.`
- Informational warning from aggregate internals.
- The pipeline still completed successfully.
