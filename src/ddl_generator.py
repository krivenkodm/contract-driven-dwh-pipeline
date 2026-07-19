import os
import re
from pathlib import Path
from typing import Any

from contract_registry import load_contracts


TYPE_MAPPING = {
    "string": "varchar",
    "timestamp": "timestamp",
}

DECIMAL_PATTERN = re.compile(
    r"decimal\((\d+),(\d+)\)"
)

IDENTIFIER_PATTERN = re.compile(
    r"^[a-z_][a-z0-9_]*$"
)


def validate_identifier(identifier: str) -> None:
    if not IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(
            f"Invalid SQL identifier: {identifier}"
        )


def to_sql_type(contract_type: str) -> str:
    if contract_type.startswith("decimal"):
        match = DECIMAL_PATTERN.fullmatch(contract_type)

        if not match:
            raise ValueError(
                f"Invalid decimal type: {contract_type}"
            )

        precision, scale = match.groups()

        return f"numeric({precision},{scale})"

    if contract_type not in TYPE_MAPPING:
        raise ValueError(
            f"Unsupported contract type: {contract_type}"
        )

    return TYPE_MAPPING[contract_type]


def generate_raw_ddl(
    contract: dict[str, Any],
) -> str:
    contract_name = contract["name"]
    validate_identifier(contract_name)

    table_name = f"raw_{contract_name}"

    columns = [
        "kafka_topic varchar NOT NULL",
        "kafka_partition integer NOT NULL",
        "kafka_offset bigint NOT NULL",
        "kafka_load_dttm timestamp NOT NULL",
    ]

    for field in contract["schema"]["fields"]:
        field_name = field["name"]
        validate_identifier(field_name)

        sql_type = to_sql_type(field["type"])
        nullable = field.get("nullable", True)

        not_null = " NOT NULL" if not nullable else ""

        columns.append(
            f"{field_name} {sql_type}{not_null}"
        )

    columns.append(
        f"CONSTRAINT uq_{table_name}_kafka_message "
        "UNIQUE ("
        "kafka_topic, "
        "kafka_partition, "
        "kafka_offset"
        ")"
    )

    columns_sql = ",\n    ".join(columns)

    return (
        f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        f"    {columns_sql}\n"
        ");\n"
    )


def main() -> None:
    contracts_directory = Path(
        os.getenv("CONTRACTS_DIR", "contracts")
    )

    output_directory = Path("sql/raw")
    output_directory.mkdir(
        parents=True,
        exist_ok=True,
    )

    contracts = load_contracts(
        contracts_directory
    )

    for contract in contracts:
        output_path = (
            output_directory
            / f"generated_{contract['name']}.sql"
        )

        ddl = generate_raw_ddl(contract)

        output_path.write_text(
            ddl,
            encoding="utf-8",
        )

        print(
            f"DDL generated: {output_path}"
        )


if __name__ == "__main__":
    main()