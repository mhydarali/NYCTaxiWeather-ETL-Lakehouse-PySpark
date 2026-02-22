from __future__ import annotations

from argparse import Namespace
from pathlib import Path
import subprocess

from mobility_lakehouse.config import AppConfig, SparkConfig, TLCConfig, WeatherConfig
from mobility_lakehouse.pipelines.bronze_tlc import download_tlc_range
from mobility_lakehouse.pipelines.bronze_weather import ingest_weather_incremental
from mobility_lakehouse.pipelines.gold_daily_metrics import run_gold_daily_metrics
from mobility_lakehouse.pipelines.silver_enriched import run_silver_enriched
from mobility_lakehouse.pipelines.silver_trips import run_job as run_silver_trips_job
from mobility_lakehouse.pipelines.silver_weather import run_silver_weather
from mobility_lakehouse.quality.checks import run_quality_checks
from mobility_lakehouse.utils.logging import setup_logging
from mobility_lakehouse.utils.spark import create_spark_session

logger = setup_logging(__name__)

PIPELINE_STEPS = [
    "bronze_tlc",
    "bronze_weather",
    "silver_trips",
    "silver_weather",
    "silver_enriched",
    "gold_daily_metrics",
    "quality",
]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _run_docker_spark_script(script_name: str, args: list[str]) -> None:
    root = _project_root()
    submit_script = root / "scripts" / "spark-submit.sh"
    script_path = str((root / "scripts" / script_name).as_posix())
    cmd = ["bash", str(submit_script), script_path, *args]
    subprocess.run(cmd, check=True)


def _run_docker_python_script(script_name: str, args: list[str]) -> None:
    root = _project_root()
    script_path = f"/workspace/scripts/{script_name}"
    cmd = [
        "docker",
        "compose",
        "exec",
        "spark-master",
        "python",
        script_path,
        *args,
    ]
    subprocess.run(cmd, check=True, cwd=root)


def run_pipeline(args: Namespace) -> None:
    app_config = AppConfig()
    skip_steps = {item.strip() for item in (args.skip_steps or "").split(",") if item.strip()}
    mode = args.mode

    tlc_cfg = TLCConfig(
        taxi_type=args.taxi_type,
        start_month=args.start_month,
        end_month=args.end_month,
        base_url_pattern=app_config.tlc.base_url_pattern,
    )
    weather_cfg = WeatherConfig(
        latitude=args.weather_lat,
        longitude=args.weather_lon,
        timezone=args.weather_timezone,
        start_date=args.weather_start_date,
        end_date=args.weather_end_date,
        endpoint=app_config.weather.endpoint,
        hourly_variables=app_config.weather.hourly_variables,
    )

    spark_master = app_config.spark.master if mode == "local" else "spark://spark-master:7077"
    spark_cfg = SparkConfig(
        app_name=app_config.spark.app_name,
        master=spark_master,
        shuffle_partitions=app_config.spark.shuffle_partitions,
        timezone=app_config.spark.timezone,
    )

    if "bronze_tlc" not in skip_steps:
        download_tlc_range(tlc_cfg)

    if "bronze_weather" not in skip_steps:
        ingest_weather_incremental(weather_cfg)

    if "silver_trips" not in skip_steps:
        if mode == "local":
            run_silver_trips_job(spark_cfg, taxi_types=[tlc_cfg.taxi_type])
        else:
            _run_docker_spark_script(
                "silver_trips.py",
                ["--master", spark_master, "--taxi-types", tlc_cfg.taxi_type],
            )

    if "silver_weather" not in skip_steps:
        if mode == "local":
            run_silver_weather(spark_cfg)
        else:
            _run_docker_spark_script("silver_weather.py", ["--master", spark_master])

    if "silver_enriched" not in skip_steps:
        if mode == "local":
            run_silver_enriched(spark_cfg)
        else:
            _run_docker_spark_script("silver_enriched.py", ["--master", spark_master])

    if "gold_daily_metrics" not in skip_steps:
        if mode == "local":
            run_gold_daily_metrics(spark_cfg)
        else:
            _run_docker_spark_script("gold_daily_metrics.py", ["--master", spark_master])

    if "quality" not in skip_steps:
        if mode == "local":
            spark = create_spark_session(
                app_name=f"{spark_cfg.app_name}-quality",
                master=spark_cfg.master,
                shuffle_partitions=spark_cfg.shuffle_partitions,
                timezone=spark_cfg.timezone,
            )
            try:
                issues = run_quality_checks(spark)
            finally:
                spark.stop()
            if issues:
                raise RuntimeError("Quality checks failed: " + "; ".join(issues))
        else:
            _run_docker_python_script("run_quality.py", ["--master", spark_master])

    executed = [step for step in PIPELINE_STEPS if step not in skip_steps]
    logger.info("pipeline completed", extra={"mode": mode, "steps": executed})
