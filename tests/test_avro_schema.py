import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from fastavro import parse_schema

from avro_schema import (
    AVRO_NAMESPACE,
    check_avro_schema_files,
    contract_to_avro_schema,
    get_avro_subject,
    normalize_avro_event,
    prepare_event_for_avro,
    write_avro_schemas,
)
from contract_registry import get_contract, load_contracts


CONTRACTS_DIR = Path("contracts")


def test_all_generated_schemas_are_valid_avro() -> None:
    for contract in load_contracts(CONTRACTS_DIR):
        parsed = parse_schema(
            contract_to_avro_schema(contract)
        )

        assert parsed["type"] == "record"


def test_contract_versions_share_stable_subject() -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    v1 = get_contract(contracts, "order_created", 1)
    v2 = get_contract(contracts, "order_created", 2)

    assert get_avro_subject(v1) == get_avro_subject(v2)
    assert get_avro_subject(v2) == (
        f"{AVRO_NAMESPACE}.order_created"
    )


def test_nullable_added_field_has_null_default() -> None:
    contract = get_contract(
        load_contracts(CONTRACTS_DIR),
        "order_created",
        2,
    )
    schema = contract_to_avro_schema(contract)
    fields = {
        field["name"]: field
        for field in schema["fields"]
    }

    assert fields["source_channel"]["type"][0] == "null"
    assert fields["source_channel"]["default"] is None


def test_logical_types_are_generated() -> None:
    contract = get_contract(
        load_contracts(CONTRACTS_DIR),
        "order_created",
        2,
    )
    fields = {
        field["name"]: field
        for field in contract_to_avro_schema(contract)["fields"]
    }

    assert fields["amount"]["type"] == {
        "type": "bytes",
        "logicalType": "decimal",
        "precision": 12,
        "scale": 2,
    }
    assert fields["created_at"]["type"] == {
        "type": "long",
        "logicalType": "timestamp-micros",
    }
    assert fields["currency"]["type"]["type"] == "enum"


def test_event_values_round_trip_to_contract_types() -> None:
    contract = get_contract(
        load_contracts(CONTRACTS_DIR),
        "order_created",
        2,
    )
    event = {
        "order_id": "ord_1",
        "customer_id": "customer_1",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-08-01T12:00:00Z",
    }

    prepared = prepare_event_for_avro(event, contract)

    assert prepared["amount"] == Decimal("1500.5")
    assert isinstance(prepared["created_at"], datetime)

    normalized = normalize_avro_event(prepared)

    assert normalized["amount"] == Decimal("1500.5")
    assert normalized["created_at"].endswith("+00:00")


def test_schema_files_are_deterministic(tmp_path: Path) -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    generated = write_avro_schemas(contracts, tmp_path)
    first_content = {
        path.name: path.read_text(encoding="utf-8")
        for path in generated
    }
    generated_again = write_avro_schemas(contracts, tmp_path)

    assert len(generated) == len(contracts)
    assert first_content == {
        path.name: path.read_text(encoding="utf-8")
        for path in generated_again
    }

    for content in first_content.values():
        assert json.loads(content)["type"] == "record"

    assert check_avro_schema_files(contracts, tmp_path) == []


def test_schema_file_check_detects_stale_and_unexpected_files(
    tmp_path: Path,
) -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    generated = write_avro_schemas(contracts, tmp_path)
    generated[0].write_text("{}\n", encoding="utf-8")
    (tmp_path / "unexpected.v1.avsc").write_text(
        "{}\n",
        encoding="utf-8",
    )

    errors = check_avro_schema_files(contracts, tmp_path)

    assert any("stale generated schema" in error for error in errors)
    assert any(
        "unexpected generated schema" in error
        for error in errors
    )
