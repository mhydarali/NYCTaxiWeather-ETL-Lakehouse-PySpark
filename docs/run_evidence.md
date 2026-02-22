# Run Evidence

This file records a reproducible local validation run for this project.

## Run context
- Project path: `/Users/mhydarali/Documents/McGill/McGill_MMA/personal_project_2`
- Execution date: February 22, 2026
- Mode: local Spark

## Commands used

```bash
make sync
make test
make lint
make run
.venv/bin/python scripts/inspect_outputs.py --sample-size 3
```

## Validation status
- `make sync`: PASS
- `make test`: PASS
- `make lint`: PASS
- `make run`: PASS
- `inspect_outputs.py`: PASS

## Output counts from inspection
- `silver.trips`: `2,927,120`
- `silver.weather`: `72`
- `silver.enriched_trips`: `2,927,120`
- `gold.daily_metrics`: `35`

## Example partition paths observed

### `data/silver/trips`
- `pickup_date=2024-01-01/taxi_type=yellow`
- `pickup_date=2024-01-02/taxi_type=yellow`
- `pickup_date=2024-01-17/taxi_type=yellow`

### `data/silver/weather`
- `date=2024-01-01`
- `date=2024-01-02`
- `date=2024-01-03`

### `data/silver/enriched_trips`
- `pickup_date=2024-01-01`
- `pickup_date=2024-01-02`
- `pickup_date=2024-01-17`

### `data/gold/daily_metrics`
- `date=2024-01-01`
- `date=2024-01-02`
- `date=2024-01-17`

## Gold schema fields
- `date`
- `total_trips`
- `avg_fare_amount`
- `avg_total_amount`
- `avg_trip_distance`
- `surge_proxy`
- `payment_type_counts`
- `avg_temp`
- `total_precip`

## Notes on warnings
Spark may print warnings like native Hadoop library messages or row-group memory scaling messages on macOS. In this run they were non-fatal and the pipeline completed successfully.
