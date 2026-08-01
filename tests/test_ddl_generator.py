import subprocess
from pathlib import Path

from ddl_generator import generate_raw_ddl


PROJECT_ROOT = (
    Path(__file__).resolve().parents[1]
)

PYTHON = (
    PROJECT_ROOT
    / ".venv"
    / "bin"
    / "python"
)


def test_ddl_generator_runs_successfully() -> None:
    result = subprocess.run(
        [
            str(PYTHON),
            "src/ddl_generator.py",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        result.stdout
        + "\n"
        + result.stderr
    )


def test_ddl_generator_creates_expected_files() -> None:
    result = subprocess.run(
        [
            str(PYTHON),
            "src/ddl_generator.py",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr

    expected_files = [
        (
            PROJECT_ROOT
            / "sql"
            / "raw"
            / "generated_order_created.sql"
        ),
        (
            PROJECT_ROOT
            / "sql"
            / "raw"
            / "generated_order_paid.sql"
        ),
        (
            PROJECT_ROOT
            / "sql"
            / "raw"
            / "generated_order_cancelled.sql"
        ),
    ]

    for file_path in expected_files:
        assert file_path.exists(), (
            f"DDL file was not generated: "
            f"{file_path}"
        )


def test_generated_ddl_contains_kafka_metadata() -> None:
    subprocess.run(
        [
            str(PYTHON),
            "src/ddl_generator.py",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    ddl_file = (
        PROJECT_ROOT
        / "sql"
        / "raw"
        / "generated_order_created.sql"
    )

    ddl = ddl_file.read_text(
        encoding="utf-8"
    )

    assert "kafka_topic" in ddl
    assert "kafka_partition" in ddl
    assert "kafka_offset" in ddl
    assert "contract_name" in ddl
    assert "contract_version" in ddl
    assert "original_payload" in ddl


def test_generated_ddl_contains_unique_kafka_key() -> None:
    subprocess.run(
        [
            str(PYTHON),
            "src/ddl_generator.py",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    ddl_file = (
        PROJECT_ROOT
        / "sql"
        / "raw"
        / "generated_order_created.sql"
    )

    ddl = ddl_file.read_text(
        encoding="utf-8"
    ).lower()

    assert "unique" in ddl
    assert "kafka_topic" in ddl
    assert "kafka_partition" in ddl
    assert "kafka_offset" in ddl


def test_generated_ddl_uses_timezone_aware_timestamps() -> None:
    subprocess.run(
        [
            str(PYTHON),
            "src/ddl_generator.py",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )

    ddl_files = (
        PROJECT_ROOT
        / "sql"
        / "raw"
    ).glob("generated_*.sql")

    for ddl_file in ddl_files:
        ddl = ddl_file.read_text(
            encoding="utf-8"
        ).lower()

        assert "timestamptz" in ddl
        assert " timestamp " not in ddl


def test_generated_ddl_uses_contract_default() -> None:
    contract = {
        "name": "test_event",
        "version": 2,
        "schema": {
            "fields": [
                {
                    "name": "source",
                    "type": "string",
                    "nullable": False,
                    "default": "unknown",
                }
            ]
        },
    }

    ddl = generate_raw_ddl(contract)

    assert (
        "source varchar DEFAULT 'unknown' NOT NULL"
        in ddl
    )
