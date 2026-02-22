# Interview Talking Points — City Mobility Lakehouse

## 30-Second Pitch
I built a local end-to-end data pipeline that combines a large public dataset (NYC taxi trips) with a public weather API, processes both through Bronze/Silver/Gold layers with Spark, and produces a daily analytics table. The project demonstrates batch ingestion, incremental API ingestion, Spark transformations, partitioned outputs, and data quality checks in a reproducible setup.

## 90-Second Walkthrough
- I ingest monthly NYC TLC parquet files into a Bronze layer and fetch weather data from Open-Meteo incrementally using a state file.
- In Silver, I standardize and clean trips (schema harmonization, invalid value filtering, deduplication) and normalize hourly weather into typed parquet.
- I enrich trips with weather by aligning timezone and joining on pickup date plus pickup hour.
- In Gold, I aggregate daily business metrics: total trips, fare and distance averages, payment-type breakdown, a deterministic surge proxy, and weather impact fields.
- I added quality checks and tests so reruns are reliable and measurable.

## 3-Minute Deep Dive
### Business Problem
Transportation analytics needs a reliable daily view of trip volume and pricing under different weather conditions. Raw files and API payloads are not analytics-ready.

### Design
- **Bronze** keeps raw data immutable and replayable.
- **Silver** handles standardization and data quality before joins.
- **Gold** provides a compact business-facing mart.

### Implementation Details
- Batch ingestion uses resumable downloads (skip existing files).
- API ingestion tracks progress with `data/bronze/weather/_state.json`.
- Spark cleanup rules enforce deterministic filters and dedupe keys.
- Enrichment join uses `pickup_date` + `pickup_hour` with timezone alignment (`America/New_York`).
- Gold output is partitioned by `date` for efficient analytics reads.

### Validation
- Automated checks: row count non-zero, null-rate thresholds, value ranges, and uniqueness.
- Tests cover URL building, weather state logic, and Spark transforms.
- Sample run produced 2.9M+ cleaned trip rows and a daily gold mart.

## STAR Story (Challenge: Schema Harmonization)
### Situation
TLC files vary by taxi type (yellow vs green), with different timestamp and location column names/casing.

### Task
Build one cleaning pipeline that works across schemas without fragile one-off branches.

### Action
Implemented flexible column resolution in the Silver trips transform and casted all required fields into a unified schema before filtering/deduplication.

### Result
A single Spark transform supports heterogeneous TLC inputs and consistently outputs a stable Silver schema for downstream joins and aggregations.

## Top 10 Interview Questions + Concise Answers
1. **Why Bronze/Silver/Gold?**
   It separates raw ingestion, cleanup, and business analytics concerns for traceability and reliability.

2. **How is ingestion idempotent?**
   TLC skips existing non-empty files; weather ingestion uses a last-ingested state file.

3. **How do you handle schema drift?**
   Column lookup/normalization in Silver trips maps variant source columns to a stable target schema.

4. **How do you ensure join correctness?**
   Same timezone for API and Spark session; join keys are pickup date and pickup hour.

5. **What quality checks are included?**
   Non-zero counts, null-rate thresholds, value-range rules, and uniqueness checks.

6. **What makes it scalable?**
   Spark processing + partitioned parquet outputs by business-relevant date keys.

7. **How do you test without external dependencies?**
   Mocked API sessions and small in-memory Spark DataFrames for transform tests.

8. **What is one tradeoff you made?**
   Kept checks lightweight and dependency-minimal instead of integrating a heavier framework.

9. **What would you improve next?**
   Add orchestrator scheduling, monitoring/alerts, and cloud object storage target.

10. **How does this map to a junior DE role?**
    It demonstrates practical ingestion, transformation, testing, and data quality workflows used in production teams.

## What I Would Improve Next
- Add scheduled orchestration with retry policies.
- Add data freshness and pipeline SLA monitoring.
- Add downstream dashboard layer for business users.
- Add cloud deployment variant (S3 + managed Spark) while preserving same layer contracts.
