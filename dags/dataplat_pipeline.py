"""Airflow orchestration for extract, dbt, Spark and publish."""

from __future__ import annotations

import subprocess
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

PROJECT = Path(__file__).resolve().parents[1]


@dag(
    dag_id="dataplat_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dataplat", "reference"],
)
def dataplat_pipeline():
    @task
    def extract() -> dict[str, str]:
        from dataplat.generate import generate

        paths = generate(PROJECT / "data" / "raw", rows=5000, seed=42)
        return {name: str(path) for name, path in paths.items()}

    @task
    def dbt_build(extracted: dict[str, str]) -> str:
        result = subprocess.run(
            ["dbt", "build", "--profiles-dir", "."],
            cwd=PROJECT / "dbt_project",
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout[-2000:]

    @task
    def spark_transform(extracted: dict[str, str]) -> str:
        from spark.spark_transform import build_session, transform

        spark = build_session("dataplat-airflow")
        try:
            frame = transform(spark, PROJECT / "data" / "raw", PROJECT / "data" / "spark")
            return f"{frame.count()} daily rows"
        finally:
            spark.stop()

    @task
    def publish(extracted: dict[str, str]) -> int:
        from dataplat.warehouse import DuckDBWarehouse

        rows = DuckDBWarehouse(PROJECT / "data" / "warehouse.duckdb").daily_revenue()
        return len(rows)

    extracted = extract()
    extracted >> dbt_build(extracted) >> spark_transform(extracted) >> publish(extracted)


dataplat_pipeline()
