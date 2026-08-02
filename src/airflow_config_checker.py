#!/usr/bin/env python3
"""Validate the Stage 9 Airflow topology without requiring Airflow locally."""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Sequence

import yaml


REQUIRED_SERVICES = {
    "airflow-postgres",
    "airflow-init",
    "airflow-api-server",
    "airflow-scheduler",
    "airflow-dag-processor",
}
REQUIRED_TASK_IDS = {
    "check_schema_registry",
    "run_analytics",
    "check_pipeline_health",
}


def _literal_keyword(call: ast.Call, name: str) -> object:
    for keyword in call.keywords:
        if keyword.arg == name:
            return ast.literal_eval(keyword.value)
    raise ValueError(f"DAG is missing {name}=...")


def validate_dag(dag_path: Path) -> list[str]:
    errors: list[str] = []
    if not dag_path.is_file():
        return [f"missing DAG: {dag_path}"]

    source = dag_path.read_text(encoding="utf-8")
    try:
        tree = ast.parse(source, filename=str(dag_path))
    except SyntaxError as exc:
        return [f"invalid DAG syntax: {exc}"]

    constants = {
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    for task_id in sorted(REQUIRED_TASK_IDS):
        if task_id not in constants:
            errors.append(f"DAG is missing task_id={task_id!r}")
    if "airflow" not in constants:
        errors.append("DAG must label analytics runs with trigger_type='airflow'")

    dag_calls = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == "DAG")
            or (isinstance(node.func, ast.Attribute) and node.func.attr == "DAG")
        )
    ]
    if len(dag_calls) != 1:
        errors.append(f"expected exactly one DAG declaration, found {len(dag_calls)}")
        return errors

    try:
        if _literal_keyword(dag_calls[0], "catchup") is not False:
            errors.append("DAG catchup must be disabled")
        if _literal_keyword(dag_calls[0], "max_active_runs") != 1:
            errors.append("DAG max_active_runs must be 1")
    except (ValueError, TypeError) as exc:
        errors.append(str(exc))

    dependency = "registry_check >> analytics_run >> health_check"
    if dependency not in source:
        errors.append("DAG task dependency chain is incomplete")
    return errors


def validate_compose(compose_path: Path) -> list[str]:
    errors: list[str] = []
    if not compose_path.is_file():
        return [f"missing Compose file: {compose_path}"]

    config = yaml.safe_load(compose_path.read_text(encoding="utf-8"))
    services = config.get("services", {})
    missing = REQUIRED_SERVICES.difference(services)
    if missing:
        errors.append(f"Compose is missing Airflow services: {', '.join(sorted(missing))}")
        return errors

    for service_name in sorted(REQUIRED_SERVICES):
        profiles = services[service_name].get("profiles", [])
        if "airflow" not in profiles:
            errors.append(f"{service_name} must use the airflow profile")

    api_ports = services["airflow-api-server"].get("ports", [])
    if not any(str(port).startswith("127.0.0.1:") for port in api_ports):
        errors.append("Airflow API server must bind to localhost")

    environment = services["airflow-scheduler"].get("environment", {})
    if environment.get("AIRFLOW__CORE__EXECUTOR") != "LocalExecutor":
        errors.append("Airflow scheduler must use LocalExecutor")
    if environment.get("AIRFLOW__CORE__PARALLELISM") != "2":
        errors.append("Airflow LocalExecutor parallelism must be limited to 2")
    if environment.get("AIRFLOW__CORE__LOAD_EXAMPLES") not in (False, "false"):
        errors.append("Airflow example DAGs must be disabled")
    jwt_secret = str(environment.get("AIRFLOW__API_AUTH__JWT_SECRET", ""))
    if len(jwt_secret) < 64:
        errors.append("Airflow JWT secret must contain at least 64 characters")
    return errors


def validate_airflow_config(project_root: Path) -> list[str]:
    errors = validate_dag(project_root / "airflow" / "dags" / "contract_dwh_analytics.py")
    errors.extend(validate_compose(project_root / "docker-compose.yml"))
    dockerfile = project_root / "Dockerfile.airflow"
    if not dockerfile.is_file():
        errors.append("missing Dockerfile.airflow")
    elif "asyncpg" not in dockerfile.read_text(encoding="utf-8"):
        errors.append("Dockerfile.airflow must install asyncpg for Airflow 3 metadata access")
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(argv or sys.argv[1:])
    project_root = Path(arguments[0]).resolve() if arguments else Path(__file__).resolve().parents[1]
    errors = validate_airflow_config(project_root)
    if errors:
        print("Airflow configuration check failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Airflow configuration check passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
