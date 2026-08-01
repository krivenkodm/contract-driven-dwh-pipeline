import os
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class IntegrationDatabase:
    name: str
    dsn: str


DatabaseFactory = Callable[[], IntegrationDatabase]


def run_database_make_target(
    database: IntegrationDatabase,
    target: str,
) -> str:
    result = subprocess.run(
        [
            "make",
            f"POSTGRES_DB={database.name}",
            target,
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.fail(
            f"make {target} failed for {database.name}:\n"
            f"{result.stdout}\n{result.stderr}"
        )

    return result.stdout + result.stderr


def run_dbt(
    database: IntegrationDatabase,
    *arguments: str,
) -> str:
    result = subprocess.run(
        [
            str(PROJECT_ROOT / ".venv" / "bin" / "dbt"),
            *arguments,
            "--project-dir",
            str(PROJECT_ROOT / "dbt"),
            "--profiles-dir",
            str(PROJECT_ROOT / "dbt"),
            "--no-use-colors",
        ],
        cwd=PROJECT_ROOT,
        env={
            **os.environ,
            "DBT_DATABASE": database.name,
        },
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode != 0:
        pytest.fail(
            f"dbt {' '.join(arguments)} failed for {database.name}:\n"
            f"{result.stdout}\n{result.stderr}"
        )

    return result.stdout + result.stderr
