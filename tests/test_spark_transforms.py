from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark", reason="pyspark is required for Spark transform tests")

from mobility_lakehouse.pipelines.gold_daily_metrics import build_daily_metrics
from mobility_lakehouse.pipelines.silver_enriched import join_trips_weather
from mobility_lakehouse.pipelines.silver_trips import clean_trips


def test_clean_trips_filters_and_deduplicates(spark):
    rows = [
        {
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-01-01 00:10:00",
            "tpep_dropoff_datetime": "2024-01-01 00:25:00",
            "PULocationID": 10,
            "DOLocationID": 20,
            "trip_distance": 2.5,
            "fare_amount": 9.0,
            "total_amount": 12.0,
            "payment_type": 1,
        },
        {
            "VendorID": 1,
            "tpep_pickup_datetime": "2024-01-01 00:10:00",
            "tpep_dropoff_datetime": "2024-01-01 00:25:00",
            "PULocationID": 10,
            "DOLocationID": 20,
            "trip_distance": 2.5,
            "fare_amount": 9.0,
            "total_amount": 12.0,
            "payment_type": 1,
        },
        {
            "VendorID": 2,
            "tpep_pickup_datetime": "2024-01-01 01:00:00",
            "tpep_dropoff_datetime": "2024-01-01 01:10:00",
            "PULocationID": 11,
            "DOLocationID": 21,
            "trip_distance": 1.0,
            "fare_amount": -1.0,
            "total_amount": 3.0,
            "payment_type": 2,
        },
    ]

    df = spark.createDataFrame(rows)
    cleaned = clean_trips(df, taxi_type="yellow")

    assert cleaned.count() == 1
    record = cleaned.first().asDict()
    assert record["trip_distance"] >= 0
    assert record["fare_amount"] >= 0
    assert record["pickup_hour"] == 0
    assert record["taxi_type"] == "yellow"


def test_join_trips_weather_left_join_behavior(spark):
    trips = spark.createDataFrame(
        [
            {
                "pickup_date": datetime(2024, 1, 1).date(),
                "pickup_hour": 0,
                "fare_amount": 10.0,
                "total_amount": 12.0,
                "trip_distance": 2.0,
            },
            {
                "pickup_date": datetime(2024, 1, 1).date(),
                "pickup_hour": 2,
                "fare_amount": 20.0,
                "total_amount": 25.0,
                "trip_distance": 3.0,
            },
        ]
    )
    weather = spark.createDataFrame(
        [
            {
                "date": datetime(2024, 1, 1).date(),
                "hour": 0,
                "temperature_2m": 5.0,
                "precipitation": 0.2,
                "windspeed_10m": 10.0,
            }
        ]
    )

    joined = join_trips_weather(trips, weather).orderBy("pickup_hour")
    out = [row.asDict() for row in joined.collect()]

    assert out[0]["temperature_2m"] == 5.0
    assert out[1]["temperature_2m"] is None


def test_build_daily_metrics_expected_outputs(spark):
    enriched = spark.createDataFrame(
        [
            {
                "pickup_date": datetime(2024, 1, 1).date(),
                "pickup_hour": 0,
                "fare_amount": 10.0,
                "total_amount": 12.0,
                "trip_distance": 2.0,
                "payment_type": 1,
                "temperature_2m": 10.0,
                "precipitation": 1.0,
            },
            {
                "pickup_date": datetime(2024, 1, 1).date(),
                "pickup_hour": 1,
                "fare_amount": 20.0,
                "total_amount": 24.0,
                "trip_distance": 4.0,
                "payment_type": 2,
                "temperature_2m": 14.0,
                "precipitation": 0.5,
            },
        ]
    )

    metrics = build_daily_metrics(enriched)
    assert metrics.count() == 1

    row = metrics.first().asDict()
    assert row["total_trips"] == 2
    assert round(row["avg_fare_amount"], 3) == 15.0
    assert round(row["avg_total_amount"], 3) == 18.0
    assert round(row["avg_trip_distance"], 3) == 3.0
    assert round(row["surge_proxy"], 3) == 6.0
    assert round(row["avg_temp"], 3) == 12.0
    assert round(row["total_precip"], 3) == 1.5

    counts = row["payment_type_counts"]
    assert counts["1"] == 1
    assert counts["2"] == 1
