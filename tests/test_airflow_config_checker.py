from pathlib import Path

from airflow_config_checker import validate_airflow_config, validate_dag


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_stage_9_airflow_configuration_is_complete() -> None:
    assert validate_airflow_config(PROJECT_ROOT) == []


def test_invalid_dag_is_reported(tmp_path: Path) -> None:
    dag_path = tmp_path / "broken.py"
    dag_path.write_text("this is not valid Python!", encoding="utf-8")

    errors = validate_dag(dag_path)

    assert len(errors) == 1
    assert errors[0].startswith("invalid DAG syntax:")
