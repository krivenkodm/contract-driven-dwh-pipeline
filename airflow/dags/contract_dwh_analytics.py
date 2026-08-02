"""Production-style orchestration for the contract-driven DWH analytics layer."""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.sdk import DAG, task


DAG_ID = "contract_dwh_analytics"
APP_DIR = Path(os.getenv("CONTRACT_DWH_APP_DIR", "/opt/contract-dwh"))
PYTHON = sys.executable


def run_command(*arguments: str) -> None:
    """Run a project command and stream its output into the Airflow task log."""
    subprocess.run(
        list(arguments),
        cwd=APP_DIR,
        env=os.environ.copy(),
        check=True,
    )


with DAG(
    dag_id=DAG_ID,
    description="Schema Registry validation, dbt build and pipeline health verification",
    schedule=os.getenv("AIRFLOW_ANALYTICS_SCHEDULE", "*/5 * * * *"),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["contract-driven", "dwh", "dbt"],
) as dag:

    @task(
        task_id="check_schema_registry",
        retries=2,
        retry_delay=timedelta(seconds=20),
        execution_timeout=timedelta(minutes=2),
    )
    def check_schema_registry() -> None:
        run_command(
            PYTHON,
            str(APP_DIR / "src" / "schema_registry_manager.py"),
            "--check",
            "--wait-attempts",
            "3",
            "--wait-delay-seconds",
            "2",
        )

    @task(
        task_id="run_analytics",
        retries=1,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
    )
    def run_analytics() -> None:
        run_command(
            PYTHON,
            str(APP_DIR / "src" / "analytics_runner.py"),
            "--trigger",
            "airflow",
            "--dbt-executable",
            "dbt",
            "--project-dir",
            str(APP_DIR / "dbt"),
            "--profiles-dir",
            str(APP_DIR / "dbt"),
        )

    @task(
        task_id="check_pipeline_health",
        retries=1,
        retry_delay=timedelta(seconds=15),
        execution_timeout=timedelta(minutes=2),
    )
    def check_pipeline_health() -> None:
        run_command(
            PYTHON,
            str(APP_DIR / "src" / "pipeline_health_check.py"),
        )

    registry_check = check_schema_registry()
    analytics_run = run_analytics()
    health_check = check_pipeline_health()

    registry_check >> analytics_run >> health_check
