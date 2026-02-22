from __future__ import annotations

import os
import sys

import pytest


@pytest.fixture(scope="session")
def spark():
    pyspark = pytest.importorskip("pyspark.sql", reason="pyspark is required for Spark transform tests")
    SparkSession = pyspark.SparkSession
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    try:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("mobility-lakehouse-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "America/New_York")
            .getOrCreate()
        )
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Spark session unavailable in this environment: {exc}")
    yield session
    session.stop()
