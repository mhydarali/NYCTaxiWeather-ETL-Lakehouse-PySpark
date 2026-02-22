# Data Sources

## 1) NYC TLC Trip Records (Large Dataset)
- Source page: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- Public download pattern used:
  - `https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month}.parquet`
- Why used in this project:
  - Large, real-world transportation data
  - Includes practical columns for analytics (times, locations, fares, payment type)
  - Useful to demonstrate batch ingestion and Spark scaling patterns

## 2) Open-Meteo Archive API (Public API)
- Docs: [Open-Meteo Documentation](https://open-meteo.com/en/docs)
- Endpoint used:
  - `https://archive-api.open-meteo.com/v1/archive`
- Hourly variables used by default:
  - `temperature_2m`, `precipitation`, `wind_speed_10m`
- Why used in this project:
  - Demonstrates incremental API ingestion
  - Provides external context for trip behavior analysis
  - Enables weather-impact features in the Gold layer

## Data Usage in Pipeline
- TLC data drives trip-level mobility metrics.
- Weather data enriches trips at hourly granularity.
- Combined output supports daily operational analysis.

## Attribution and Licensing
- NYC TLC: see source page for official terms and reuse conditions.
- Open-Meteo: see official docs for attribution/license requirements.
