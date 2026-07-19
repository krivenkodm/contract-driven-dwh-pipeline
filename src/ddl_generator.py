from pathlib import Path
import re
import yaml


TYPE_MAPPING = {
    "string": "varchar",
    "timestamp": "timestamp",
}


def load_contract(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def to_sql_type(contract_type: str) -> str:
    if contract_type.startswith("decimal"):
        match = re.match(r"decimal\((\d+),(\d+)\)", contract_type)
        if not match:
            raise ValueError(f"Invalid decimal type: {contract_type}")

        precision, scale = match.groups()
        return f"numeric({precision},{scale})"

    if contract_type not in TYPE_MAPPING:
        raise ValueError(f"Unsupported type: {contract_type}")

    return TYPE_MAPPING[contract_type]


def generate_raw_ddl(contract: dict) -> str:
    table_name = f"raw_{contract['name']}"

    columns = [
        "kafka_topic varchar NOT NULL",
        "kafka_partition integer NOT NULL",
        "kafka_offset bigint NOT NULL",
        "kafka_load_dttm timestamp NOT NULL",
    ]

    for field in contract["schema"]["fields"]:
        name = field["name"]
        sql_type = to_sql_type(field["type"])
        nullable = field.get("nullable", True)

        not_null = " NOT NULL" if not nullable else ""
        columns.append(f"{name} {sql_type}{not_null}")

    unique_constraint = (
        f"CONSTRAINT uq_{table_name}_kafka_offset "
        f"UNIQUE (kafka_topic, kafka_partition, kafka_offset)"
    )

    columns.append(unique_constraint)

    columns_sql = ",\n    ".join(columns)

    return f"""CREATE TABLE IF NOT EXISTS {table_name} (
    {columns_sql}
);
"""


def main() -> None:
    contract_path = "contracts/order_created.v1.yaml"
    output_path = "sql/raw/generated_order_created.sql"

    contract = load_contract(contract_path)
    ddl = generate_raw_ddl(contract)

    Path(output_path).write_text(ddl, encoding="utf-8")

    print(f"DDL generated: {output_path}")


if __name__ == "__main__":
    main()