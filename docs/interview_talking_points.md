# Interview Talking Points — NYC Taxi + Weather Lakehouse

## 30-second version
I built a local end-to-end data pipeline that combines NYC taxi trips with hourly weather data. The pipeline ingests raw data, cleans and standardizes it in Spark, joins weather to trips, and outputs a Gold daily metrics table. I also added tests and quality checks so reruns are reliable.

## 90-second walkthrough
- Bronze:
  - Download monthly TLC parquet files.
  - Pull weather from Open-Meteo and track progress with a state file.
- Silver:
  - Standardize trip schema across TLC variations.
  - Filter bad records and deduplicate.
  - Normalize weather into typed hourly records.
- Enrichment:
  - Join on `pickup_date` + `pickup_hour` after timezone alignment.
- Gold:
  - Build daily metrics (trip volume, fare/distance averages, payment mix, weather fields).
- Reliability:
  - Run tests, lint, and quality checks from simple CLI commands.

## 3-minute deep dive
### Problem
Taxi data is large and messy, and weather comes from a separate API. If analysts combine this manually, it is slow and error-prone.

### What I built
A local medallion pipeline (`bronze -> silver -> gold`) with stage-specific scripts and clear contracts between layers.

### Key technical points
- Resumable batch ingestion for monthly parquet files
- Incremental weather ingestion using `data/bronze/weather/_state.json`
- Schema harmonization for TLC column variations
- Spark transformations and partitioned parquet outputs
- Deterministic enrichment join on hour-level keys

### Evidence
In a recent run:
- `silver.trips`: 2,927,120 rows
- `silver.weather`: 72 rows
- `silver.enriched_trips`: 2,927,120 rows
- `gold.daily_metrics`: 35 rows

## STAR example (schema differences)
### Situation
TLC input files vary by taxi type and can use different names for equivalent columns.

### Task
Create one transform that handles schema variation without brittle one-off logic.

### Action
I added column resolution and explicit type casting into a single target schema before filtering and deduplicating.

### Result
The same Silver trips job can process heterogeneous TLC inputs and produce a stable downstream contract.

## Questions I expect in interviews
1. Why use Bronze/Silver/Gold?
It separates raw ingestion, cleanup, and analytics logic so debugging and reprocessing are easier.

2. How do you make ingestion idempotent?
TLC download skips existing files; weather ingestion records last ingested date in a state file.

3. How do you handle schema drift?
I resolve known source column variants and cast to a canonical Silver schema.

4. How do you validate joins?
Both sources use `America/New_York`; join keys are `pickup_date` and `pickup_hour`.

5. What quality checks are included?
Non-empty outputs, null-rate thresholds, range checks, and uniqueness checks.

6. What would you improve next?
Scheduling, alerting, and a cloud deployment target while keeping the same layer contracts.

## Honest next steps
- Add orchestrated scheduling and retry logic
- Add freshness/SLA alerts
- Add a small dashboard query layer for business users
