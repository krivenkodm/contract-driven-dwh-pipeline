import argparse
import json
import os
import re
from collections.abc import Iterable
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from contract_registry import load_contracts


Contract = dict[str, Any]

AVRO_NAMESPACE = "io.contract_dwh.events"
DECIMAL_PATTERN = re.compile(r"decimal\((\d+),(\d+)\)")


def get_avro_subject(contract: Contract) -> str:
    """Return the stable subject shared by all contract versions."""

    return f"{AVRO_NAMESPACE}.{contract['name']}"


def _decimal_parameters(contract_type: str) -> tuple[int, int]:
    match = DECIMAL_PATTERN.fullmatch(contract_type)

    if match is None:
        raise ValueError(
            f"Unsupported decimal type: {contract_type}"
        )

    return int(match.group(1)), int(match.group(2))


def _avro_base_type(
    field: dict[str, Any],
) -> str | dict[str, Any]:
    field_type = field["type"]

    if field_type == "string":
        symbols = field.get("enum")

        if symbols is None:
            return "string"

        return {
            "type": "enum",
            "name": f"{field['name']}_enum",
            "symbols": symbols,
        }

    if field_type == "timestamp":
        return {
            "type": "long",
            "logicalType": "timestamp-micros",
        }

    if field_type.startswith("decimal"):
        precision, scale = _decimal_parameters(field_type)

        return {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": precision,
            "scale": scale,
        }

    raise ValueError(f"Unsupported contract type: {field_type}")


def _decimal_default(value: Any, scale: int) -> str:
    decimal_value = Decimal(str(value))
    unscaled_value = int(
        decimal_value * (Decimal(10) ** scale)
    )

    if unscaled_value == 0:
        encoded = b"\x00"
    else:
        byte_length = max(
            1,
            (unscaled_value.bit_length() + 8) // 8,
        )
        encoded = unscaled_value.to_bytes(
            byte_length,
            byteorder="big",
            signed=True,
        )

        while len(encoded) > 1:
            if encoded[:2] == b"\x00\x00":
                encoded = encoded[1:]
            elif encoded[:2] == b"\xff\xff":
                encoded = encoded[1:]
            else:
                break

    return encoded.decode("latin-1")


def _avro_default(field: dict[str, Any]) -> Any:
    value = field["default"]
    field_type = field["type"]

    if value is None or field_type == "string":
        return value

    if field_type == "timestamp":
        parsed = datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
        utc_value = parsed.astimezone(timezone.utc)
        return int(utc_value.timestamp() * 1_000_000)

    if field_type.startswith("decimal"):
        _, scale = _decimal_parameters(field_type)
        return _decimal_default(value, scale)

    raise ValueError(
        f"Unsupported default for contract type: {field_type}"
    )


def _avro_field(field: dict[str, Any]) -> dict[str, Any]:
    base_type = _avro_base_type(field)
    nullable = field["nullable"]
    has_default = "default" in field

    result: dict[str, Any] = {
        "name": field["name"],
    }

    if nullable:
        if has_default and field["default"] is not None:
            result["type"] = [base_type, "null"]
            result["default"] = _avro_default(field)
        else:
            result["type"] = ["null", base_type]
            result["default"] = None
    else:
        result["type"] = base_type

        if has_default:
            result["default"] = _avro_default(field)

    if "description" in field:
        result["doc"] = field["description"]

    return result


def contract_to_avro_schema(
    contract: Contract,
) -> dict[str, Any]:
    """Generate a deterministic Avro record from a YAML contract."""

    return {
        "type": "record",
        "name": contract["name"],
        "namespace": AVRO_NAMESPACE,
        "doc": (
            f"Generated from {contract['name']} "
            f"contract v{contract['version']}; "
            f"owner={contract['owner']}"
        ),
        "fields": [
            _avro_field(field)
            for field in contract["schema"]["fields"]
        ],
    }


def avro_schema_json(contract: Contract) -> str:
    return json.dumps(
        contract_to_avro_schema(contract),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )


def prepare_event_for_avro(
    event: dict[str, Any],
    contract: Contract,
) -> dict[str, Any]:
    """Convert contract-native values into fastavro logical values."""

    prepared = dict(event)

    for field in contract["schema"]["fields"]:
        field_name = field["name"]

        if field_name not in prepared:
            continue

        value = prepared[field_name]

        if value is None:
            continue

        field_type = field["type"]

        if field_type == "timestamp" and isinstance(value, str):
            prepared[field_name] = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        elif field_type.startswith("decimal"):
            prepared[field_name] = Decimal(str(value))

    return prepared


def normalize_avro_event(
    event: dict[str, Any],
) -> dict[str, Any]:
    """Convert decoded Avro values back to contract-native values."""

    normalized: dict[str, Any] = {}

    for field_name, value in event.items():
        if isinstance(value, datetime):
            normalized[field_name] = value.isoformat()
        else:
            normalized[field_name] = value

    return normalized


def _formatted_schema(contract: Contract) -> str:
    return (
        json.dumps(
            contract_to_avro_schema(contract),
            ensure_ascii=True,
            indent=2,
        )
        + "\n"
    )


def write_avro_schemas(
    contracts: Iterable[Contract],
    output_directory: str | Path,
) -> list[Path]:
    output_directory = Path(output_directory)
    output_directory.mkdir(parents=True, exist_ok=True)

    generated_paths: list[Path] = []

    for contract in contracts:
        output_path = output_directory / (
            f"{contract['name']}.v{contract['version']}.avsc"
        )
        output_path.write_text(
            _formatted_schema(contract),
            encoding="utf-8",
        )
        generated_paths.append(output_path)

    return generated_paths


def check_avro_schema_files(
    contracts: Iterable[Contract],
    output_directory: str | Path,
) -> list[str]:
    output_directory = Path(output_directory)
    expected_files = {
        f"{contract['name']}.v{contract['version']}.avsc": (
            _formatted_schema(contract)
        )
        for contract in contracts
    }
    actual_files = {
        path.name: path
        for path in output_directory.glob("*.avsc")
    }
    errors: list[str] = []

    for filename, expected_content in expected_files.items():
        path = actual_files.get(filename)

        if path is None:
            errors.append(f"missing generated schema: {filename}")
        elif path.read_text(encoding="utf-8") != expected_content:
            errors.append(f"stale generated schema: {filename}")

    for filename in sorted(actual_files.keys() - expected_files.keys()):
        errors.append(f"unexpected generated schema: {filename}")

    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-directory",
        default="schemas/avro",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify that generated schema files are current",
    )
    args = parser.parse_args()

    contracts = load_contracts(
        os.getenv("CONTRACTS_DIR", "contracts")
    )
    if args.check:
        errors = check_avro_schema_files(
            contracts=contracts,
            output_directory=args.output_directory,
        )

        if errors:
            print("Generated Avro schema check failed:")

            for error in errors:
                print(f"  - {error}")

            raise SystemExit(1)

        print("Generated Avro schema check passed")
        return

    generated_paths = write_avro_schemas(
        contracts=contracts,
        output_directory=args.output_directory,
    )

    for output_path in generated_paths:
        print(f"Avro schema generated: {output_path}")


if __name__ == "__main__":
    main()
