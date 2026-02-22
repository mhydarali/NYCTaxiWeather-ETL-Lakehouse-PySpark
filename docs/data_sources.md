# Data Sources

## 1) NYC TLC Trip Records
- Source page: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Download pattern used in this project:
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet`

Why this source:
- Real production-scale transportation data
- Useful trip features (timestamps, locations, fare/total, payment type)
- Good fit for demonstrating batch ingestion and Spark transforms

## 2) Open-Meteo Archive API
- Docs: [Open-Meteo Documentation](https://open-meteo.com/en/docs)
- Endpoint:
  - `https://archive-api.open-meteo.com/v1/archive`
- Hourly variables used:
  - `temperature_2m`, `precipitation`, `wind_speed_10m`

Why this source:
- Demonstrates incremental API ingestion with state tracking
- Adds external context to trip behavior
- Enables weather-aware features in Gold outputs

## How the project uses these together
- TLC drives trip-level mobility metrics
- Weather enriches trip records by hour
- Gold layer aggregates both into daily business metrics

## Attribution
- NYC TLC terms and reuse: see the official TLC source page
- Open-Meteo attribution/license: see Open-Meteo docs
