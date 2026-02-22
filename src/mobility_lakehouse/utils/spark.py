from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    shuffle_partitions: int = 8,
    timezone: str = "America/New_York",
) -> SparkSession:
    worker_python = os.getenv("PYSPARK_PYTHON", sys.executable)
    driver_python = os.getenv("PYSPARK_DRIVER_PYTHON", sys.executable)
    os.environ["PYSPARK_PYTHON"] = worker_python
    os.environ["PYSPARK_DRIVER_PYTHON"] = driver_python
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.session.timeZone", timezone)
        .config("spark.pyspark.python", worker_python)
        .config("spark.pyspark.driver.python", driver_python)
        .getOrCreate()
    )
