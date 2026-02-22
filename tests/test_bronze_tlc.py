from mobility_lakehouse.pipelines.bronze_tlc import build_tlc_url


def test_build_tlc_url():
    url = build_tlc_url(
        taxi_type="yellow",
        year=2024,
        month=1,
        base_url_pattern=(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/"
            "{taxi_type}_tripdata_{year}-{month:02d}.parquet"
        ),
    )
    assert url == "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
