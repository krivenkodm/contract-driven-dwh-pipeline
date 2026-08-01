import os
import re
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
from typing import Any

from compatibility_checker import (
    check_all_contracts,
)
from contract_registry import load_contracts


TYPE_MAPPING = {
    "string": "varchar",
    "timestamp": "timestamptz",
}

DECIMAL_PATTERN = re.compile(
    r"decimal\((\d+),(\d+)\)"
)

IDENTIFIER_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*$"
)


Contract = dict[str, Any]


def validate_identifier(
    identifier: str,
) -> None:
    if not IDENTIFIER_PATTERN.fullmatch(
        identifier
    ):
        raise ValueError(
            f"Invalid SQL identifier: {identifier}"
        )


def to_sql_type(
    contract_type: str,
) -> str:
    if contract_type.startswith("decimal"):
        match = DECIMAL_PATTERN.fullmatch(
            contract_type
        )

        if not match:
            raise ValueError(
                "Invalid decimal type: "
                f"{contract_type}"
            )

        precision, scale = match.groups()

        return (
            f"numeric({precision},{scale})"
        )

    if contract_type not in TYPE_MAPPING:
        raise ValueError(
            "Unsupported contract type: "
            f"{contract_type}"
        )

    return TYPE_MAPPING[contract_type]


def to_sql_literal(
    value: Any,
    contract_type: str,
) -> str:
    if value is None:
        return "NULL"

    if contract_type in {
        "string",
        "timestamp",
    }:
        escaped_value = str(value).replace(
            "'",
            "''",
        )

        return f"'{escaped_value}'"

    if contract_type.startswith("decimal"):
        return str(Decimal(str(value)))

    raise ValueError(
        "Unsupported default for contract type: "
        f"{contract_type}"
    )


def get_latest_contracts(
    contracts: list[Contract],
) -> list[Contract]:
    contracts_by_name: dict[
        str,
        list[Contract],
    ] = defaultdict(list)

    for contract in contracts:
        contracts_by_name[
            contract["name"]
        ].append(
            contract
        )

    latest_contracts: list[Contract] = []

    for name, versions in contracts_by_name.items():
        latest_contract = max(
            versions,
            key=lambda contract: contract["version"],
        )

        latest_contracts.append(
            latest_contract
        )

    return sorted(
        latest_contracts,
        key=lambda contract: contract["name"],
    )


def generate_raw_ddl(
    contract: Contract,
) -> str:
    contract_name = contract["name"]
    contract_version = contract["version"]

    validate_identifier(
        contract_name
    )

    table_name = f"raw_{contract_name}"

    columns = [
        (
            "raw_id bigint "
            "GENERATED ALWAYS AS IDENTITY "
            "PRIMARY KEY"
        ),
        "kafka_topic varchar NOT NULL",
        "kafka_partition integer NOT NULL",
        "kafka_offset bigint NOT NULL",
        "kafka_load_dttm timestamptz NOT NULL",
        "contract_name varchar NOT NULL",
        "contract_version integer NOT NULL",
        "original_payload jsonb NOT NULL",
    ]

    for field in contract["schema"]["fields"]:
        field_name = field["name"]

        validate_identifier(
            field_name
        )

        sql_type = to_sql_type(
            field["type"]
        )

        nullable = field.get(
            "nullable",
            True,
        )

        not_null = (
            " NOT NULL"
            if not nullable
            else ""
        )

        default = (
            " DEFAULT "
            + to_sql_literal(
                value=field["default"],
                contract_type=field["type"],
            )
            if "default" in field
            else ""
        )

        columns.append(
            f"{field_name} "
            f"{sql_type}"
            f"{default}"
            f"{not_null}"
        )

    columns.append(
        f"CONSTRAINT "
        f"uq_{table_name}_kafka_message "
        "UNIQUE ("
        "kafka_topic, "
        "kafka_partition, "
        "kafka_offset"
        ")"
    )

    columns_sql = ",\n    ".join(
        columns
    )

    return (
        f"-- Generated from "
        f"{contract_name} v{contract_version}\n"
        f"CREATE TABLE IF NOT EXISTS "
        f"{table_name} (\n"
        f"    {columns_sql}\n"
        ");\n"
    )


def main() -> None:
    contracts_directory = Path(
        os.getenv(
            "CONTRACTS_DIR",
            "contracts",
        )
    )

    output_directory = Path(
        "sql/raw"
    )

    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    contracts = load_contracts(
        contracts_directory
    )

    compatibility_errors = (
        check_all_contracts(
            contracts
        )
    )

    if compatibility_errors:
        formatted_errors = "\n".join(
            f"  - {error}"
            for error in compatibility_errors
        )

        raise ValueError(
            "Contract compatibility "
            "check failed:\n"
            f"{formatted_errors}"
        )

    latest_contracts = get_latest_contracts(
        contracts
    )

    for contract in latest_contracts:
        output_path = (
            output_directory
            / (
                f"generated_"
                f"{contract['name']}.sql"
            )
        )

        ddl = generate_raw_ddl(
            contract
        )

        output_path.write_text(
            ddl,
            encoding="utf-8",
        )

        print(
            "DDL generated: "
            f"{output_path} "
            f"from v{contract['version']}"
        )


if __name__ == "__main__":
    main()
