from pathlib import Path
from typing import Any

import pytest
import yaml

from contract_registry import (
    load_contract,
    load_contracts,
)


Contract = dict[str, Any]


def make_contract(
    name: str = "test_event",
    version: int = 1,
    topic: str | None = None,
) -> Contract:
    return {
        "name": name,
        "version": version,
        "topic": topic or f"test.{name}.v{version}",
        "owner": "test-team",
        "key": ["event_id"],
        "schema": {
            "allow_extra_fields": False,
            "fields": [
                {
                    "name": "event_id",
                    "type": "string",
                    "nullable": False,
                }
            ],
        },
        "quality": {
            "unique_key": ["event_id"],
            "not_null": ["event_id"],
        },
        "compatibility": {
            "mode": "backward",
        },
    }


def write_contract(
    path: Path,
    contract: Contract,
) -> None:
    path.write_text(
        yaml.safe_dump(
            contract,
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_load_contract_from_yaml(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(
        contract_path,
        make_contract(),
    )

    contract = load_contract(contract_path)

    assert contract["name"] == "test_event"
    assert contract["version"] == 1
    assert contract["topic"] == "test.test_event.v1"
    assert contract["owner"] == "test-team"


def test_load_multiple_contracts(
    tmp_path: Path,
) -> None:
    write_contract(
        tmp_path / "first.v1.yaml",
        make_contract(name="first"),
    )

    write_contract(
        tmp_path / "second.v1.yaml",
        make_contract(name="second"),
    )

    contracts = load_contracts(tmp_path)

    assert {
        contract["name"]
        for contract in contracts
    } == {"first", "second"}


def test_invalid_yaml_raises_error(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "invalid.v1.yaml"
    contract_path.write_text(
        "name: invalid\nversion: [",
        encoding="utf-8",
    )

    with pytest.raises(yaml.YAMLError):
        load_contract(contract_path)


def test_duplicate_topics_raise_error(
    tmp_path: Path,
) -> None:
    shared_topic = "test.shared.v1"

    write_contract(
        tmp_path / "first.v1.yaml",
        make_contract(
            name="first",
            topic=shared_topic,
        ),
    )

    write_contract(
        tmp_path / "second.v1.yaml",
        make_contract(
            name="second",
            topic=shared_topic,
        ),
    )

    with pytest.raises(
        ValueError,
        match="Duplicate contract topic",
    ):
        load_contracts(tmp_path)


def test_meta_schema_rejects_missing_owner(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    del contract["owner"]

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="owner.*required",
    ):
        load_contract(contract_path)


def test_meta_schema_rejects_unknown_properties(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["unexpected"] = True

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="Additional properties",
    ):
        load_contract(contract_path)


def test_duplicate_field_names_are_rejected(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["schema"]["fields"].append(
        {
            "name": "event_id",
            "type": "string",
            "nullable": False,
        }
    )

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="duplicate field names",
    ):
        load_contract(contract_path)


def test_unknown_field_references_are_rejected(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["key"] = ["missing_id"]

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="key references unknown fields",
    ):
        load_contract(contract_path)


def test_nullable_business_key_is_rejected(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["schema"]["fields"][0]["nullable"] = True

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="key field event_id must be non-nullable",
    ):
        load_contract(contract_path)


def test_invalid_decimal_definition_is_rejected(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["schema"]["fields"].append(
        {
            "name": "amount",
            "type": "decimal(2,3)",
            "nullable": False,
        }
    )

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="invalid decimal type",
    ):
        load_contract(contract_path)


def test_invalid_timestamp_default_is_rejected(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["schema"]["fields"].append(
        {
            "name": "created_at",
            "type": "timestamp",
            "nullable": False,
            "default": "2026-08-01T12:00:00",
        }
    )

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match="invalid default for type timestamp",
    ):
        load_contract(contract_path)


def test_filename_must_match_contract_identity(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "wrong_name.v1.yaml"
    write_contract(
        contract_path,
        make_contract(),
    )

    with pytest.raises(
        ValueError,
        match="filename must be test_event.v1.yaml",
    ):
        load_contract(contract_path)


def test_topic_version_must_match_contract_version(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(
        contract_path,
        make_contract(
            topic="test.test_event.v2",
        ),
    )

    with pytest.raises(
        ValueError,
        match="topic must end with .v1",
    ):
        load_contract(contract_path)


def test_quality_not_null_field_must_be_non_nullable(
    tmp_path: Path,
) -> None:
    contract = make_contract()
    contract["schema"]["fields"].append(
        {
            "name": "source",
            "type": "string",
            "nullable": True,
        }
    )
    contract["quality"]["not_null"].append("source")

    contract_path = tmp_path / "test_event.v1.yaml"
    write_contract(contract_path, contract)

    with pytest.raises(
        ValueError,
        match=(
            "quality.not_null field source "
            "must be non-nullable"
        ),
    ):
        load_contract(contract_path)


def test_contract_versions_must_be_contiguous(
    tmp_path: Path,
) -> None:
    write_contract(
        tmp_path / "test_event.v1.yaml",
        make_contract(version=1),
    )

    write_contract(
        tmp_path / "test_event.v3.yaml",
        make_contract(version=3),
    )

    with pytest.raises(
        ValueError,
        match="non-contiguous versions",
    ):
        load_contracts(tmp_path)
