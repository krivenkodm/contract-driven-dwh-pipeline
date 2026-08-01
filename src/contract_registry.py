import json
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator


Contract = dict[str, Any]

DECIMAL_PATTERN = re.compile(
    r"decimal\((\d+),(\d+)\)"
)

DEFAULT_META_SCHEMA_PATH = (
    Path(__file__).resolve().parents[1]
    / "contracts"
    / "data_contract.schema.json"
)


@lru_cache(maxsize=None)
def load_meta_schema(
    schema_path_text: str,
) -> dict[str, Any]:
    schema_path = Path(schema_path_text)

    with schema_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        meta_schema = json.load(file)

    Draft202012Validator.check_schema(
        meta_schema
    )

    return meta_schema


def get_meta_schema() -> dict[str, Any]:
    schema_path = Path(
        os.getenv(
            "CONTRACT_META_SCHEMA_PATH",
            str(DEFAULT_META_SCHEMA_PATH),
        )
    ).resolve()

    if not schema_path.is_file():
        raise ValueError(
            "Contract meta-schema does not exist: "
            f"{schema_path}"
        )

    return load_meta_schema(
        str(schema_path)
    )


def validate_meta_schema(
    contract: Contract,
    contract_path: Path,
) -> None:
    validator = Draft202012Validator(
        get_meta_schema()
    )

    errors = sorted(
        validator.iter_errors(contract),
        key=lambda error: [
            str(part)
            for part in error.absolute_path
        ],
    )

    if not errors:
        return

    first_error = errors[0]
    location = ".".join(
        str(part)
        for part in first_error.absolute_path
    ) or "<root>"

    raise ValueError(
        f"Contract {contract_path} failed meta-schema "
        f"validation at {location}: "
        f"{first_error.message}"
    )


def validate_decimal_default(
    value: Any,
    contract_type: str,
) -> bool:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float, Decimal))
    ):
        return False

    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return False

    if not decimal_value.is_finite():
        return False

    match = DECIMAL_PATTERN.fullmatch(
        contract_type
    )

    if match is None:
        return False

    precision = int(match.group(1))
    scale = int(match.group(2))

    if scale > precision:
        return False

    _, digits, exponent = decimal_value.as_tuple()

    actual_scale = max(-exponent, 0)
    integer_digits = max(
        len(digits) + exponent,
        0,
    )

    if decimal_value == 0:
        integer_digits = 1

    return (
        actual_scale <= scale
        and integer_digits <= precision - scale
    )


def validate_default(
    field: dict[str, Any],
) -> str | None:
    if "default" not in field:
        return None

    field_name = field["name"]
    field_type = field["type"]
    nullable = field["nullable"]
    default = field["default"]

    if default is None:
        if nullable:
            return None

        return (
            f"non-nullable field {field_name} "
            "cannot have a null default"
        )

    if field_type == "string":
        valid = isinstance(default, str)

    elif field_type == "timestamp":
        valid = isinstance(default, str)

        if valid:
            try:
                parsed = datetime.fromisoformat(
                    default.replace("Z", "+00:00")
                )
                valid = (
                    parsed.tzinfo is not None
                    and parsed.utcoffset() is not None
                )
            except ValueError:
                valid = False

    elif field_type.startswith("decimal"):
        valid = validate_decimal_default(
            value=default,
            contract_type=field_type,
        )

    else:
        valid = False

    if not valid:
        return (
            f"field {field_name} has an invalid "
            f"default for type {field_type}"
        )

    allowed_values = field.get("enum")

    if (
        allowed_values is not None
        and default not in allowed_values
    ):
        return (
            f"field {field_name} default is not "
            "included in its enum"
        )

    return None


def validate_contract_semantics(
    contract: Contract,
    contract_path: Path,
) -> None:
    name = contract["name"]
    version = contract["version"]
    topic = contract["topic"]
    fields = contract["schema"]["fields"]

    errors: list[str] = []

    expected_filename = (
        f"{name}.v{version}"
        f"{contract_path.suffix.lower()}"
    )

    if contract_path.name != expected_filename:
        errors.append(
            "filename must be "
            f"{expected_filename}"
        )

    if not topic.endswith(f".v{version}"):
        errors.append(
            f"topic must end with .v{version}"
        )

    field_names = [
        field["name"]
        for field in fields
    ]

    duplicate_field_names = sorted(
        {
            field_name
            for field_name in field_names
            if field_names.count(field_name) > 1
        }
    )

    if duplicate_field_names:
        errors.append(
            "duplicate field names: "
            f"{duplicate_field_names}"
        )

    fields_by_name = {
        field["name"]: field
        for field in fields
    }

    reference_groups = {
        "key": contract["key"],
        "quality.unique_key": (
            contract["quality"]["unique_key"]
        ),
        "quality.not_null": (
            contract["quality"]["not_null"]
        ),
    }

    for reference_name, references in reference_groups.items():
        missing_references = sorted(
            set(references) - set(fields_by_name)
        )

        if missing_references:
            errors.append(
                f"{reference_name} references unknown fields: "
                f"{missing_references}"
            )

    for reference_name in (
        "key",
        "quality.unique_key",
        "quality.not_null",
    ):
        for field_name in reference_groups[reference_name]:
            field = fields_by_name.get(field_name)

            if (
                field is not None
                and field["nullable"]
            ):
                errors.append(
                    f"{reference_name} field "
                    f"{field_name} must be non-nullable"
                )

    for field in fields:
        field_name = field["name"]
        field_type = field["type"]

        if field_type.startswith("decimal"):
            match = DECIMAL_PATTERN.fullmatch(
                field_type
            )

            if (
                match is None
                or int(match.group(2))
                > int(match.group(1))
            ):
                errors.append(
                    f"field {field_name} has invalid "
                    f"decimal type {field_type}"
                )

        if (
            "enum" in field
            and field_type != "string"
        ):
            errors.append(
                f"field {field_name} can use enum "
                "only with string type"
            )

        default_error = validate_default(field)

        if default_error is not None:
            errors.append(default_error)

    if errors:
        formatted_errors = "; ".join(errors)

        raise ValueError(
            f"Contract {contract_path} failed semantic "
            f"validation: {formatted_errors}"
        )


def load_contract(
    contract_path: str | Path,
) -> Contract:
    contract_path = Path(contract_path)

    if not contract_path.is_file():
        raise FileNotFoundError(
            f"Contract file does not exist: {contract_path}"
        )

    with contract_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        contract = yaml.safe_load(file)

    if not isinstance(contract, dict):
        raise ValueError(
            f"Contract must be a mapping: {contract_path}"
        )

    validate_meta_schema(
        contract=contract,
        contract_path=contract_path,
    )

    validate_contract_semantics(
        contract=contract,
        contract_path=contract_path,
    )

    return contract


def load_contracts(
    contracts_directory: str | Path,
) -> list[Contract]:
    contracts_directory = Path(
        contracts_directory
    )

    if not contracts_directory.is_dir():
        raise ValueError(
            "Contracts directory does not exist: "
            f"{contracts_directory}"
        )

    contract_paths = sorted(
        [
            *contracts_directory.glob("*.yaml"),
            *contracts_directory.glob("*.yml"),
        ]
    )

    if not contract_paths:
        raise ValueError(
            "No contract files found in: "
            f"{contracts_directory}"
        )

    contracts: list[Contract] = []
    contract_versions: set[tuple[str, int]] = set()
    topics: set[str] = set()

    for contract_path in contract_paths:
        contract = load_contract(contract_path)

        name = contract["name"]
        version = contract["version"]
        topic = contract["topic"]
        contract_version = (name, version)

        if contract_version in contract_versions:
            raise ValueError(
                "Duplicate contract version: "
                f"{name} v{version}"
            )

        if topic in topics:
            raise ValueError(
                f"Duplicate contract topic: {topic}"
            )

        contract_versions.add(contract_version)
        topics.add(topic)
        contracts.append(contract)

    contract_names = {
        contract["name"]
        for contract in contracts
    }

    for name in contract_names:
        versions = sorted(
            contract["version"]
            for contract in contracts
            if contract["name"] == name
        )

        expected_versions = list(
            range(1, max(versions) + 1)
        )

        if versions != expected_versions:
            raise ValueError(
                f"Contract {name} has non-contiguous "
                f"versions: {versions}; expected "
                f"{expected_versions}"
            )

    return sorted(
        contracts,
        key=lambda contract: (
            contract["name"],
            contract["version"],
        ),
    )


def get_contract(
    contracts: list[Contract],
    name: str,
    version: int | None = None,
) -> Contract:
    matching_contracts = [
        contract
        for contract in contracts
        if contract["name"] == name
    ]

    if not matching_contracts:
        raise ValueError(
            f"Contract not found: {name}"
        )

    if version is None:
        return max(
            matching_contracts,
            key=lambda contract: contract["version"],
        )

    for contract in matching_contracts:
        if contract["version"] == version:
            return contract

    raise ValueError(
        f"Contract not found: {name} v{version}"
    )


def map_contracts_by_topic(
    contracts: list[Contract],
) -> dict[str, Contract]:
    contracts_by_topic: dict[str, Contract] = {}

    for contract in contracts:
        topic = contract["topic"]

        if topic in contracts_by_topic:
            raise ValueError(
                f"Duplicate contract topic: {topic}"
            )

        contracts_by_topic[topic] = contract

    return contracts_by_topic


def get_contract_by_topic(
    contracts: list[Contract],
    topic: str,
) -> Contract:
    for contract in contracts:
        if contract["topic"] == topic:
            return contract

    raise ValueError(
        "Contract not found for topic: "
        f"{topic}"
    )
