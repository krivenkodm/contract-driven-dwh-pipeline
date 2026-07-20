from pathlib import Path
from typing import Any

import yaml


Contract = dict[str, Any]


def load_contract(
    contract_path: Path,
) -> Contract:
    with contract_path.open(
        "r",
        encoding="utf-8",
    ) as file:
        contract = yaml.safe_load(file)

    if not isinstance(contract, dict):
        raise ValueError(
            f"Contract must be a mapping: "
            f"{contract_path}"
        )

    required_fields = {
        "name",
        "version",
        "topic",
        "schema",
    }

    missing_fields = (
        required_fields
        - contract.keys()
    )

    if missing_fields:
        formatted_fields = ", ".join(
            sorted(missing_fields)
        )

        raise ValueError(
            f"Contract {contract_path} "
            f"is missing fields: "
            f"{formatted_fields}"
        )

    name = contract["name"]
    version = contract["version"]
    topic = contract["topic"]

    if not isinstance(name, str):
        raise ValueError(
            f"Contract name must be a string: "
            f"{contract_path}"
        )

    if not isinstance(version, int):
        raise ValueError(
            f"Contract version must be an integer: "
            f"{contract_path}"
        )

    if version <= 0:
        raise ValueError(
            f"Contract version must be positive: "
            f"{contract_path}"
        )

    if not isinstance(topic, str):
        raise ValueError(
            f"Contract topic must be a string: "
            f"{contract_path}"
        )

    schema = contract["schema"]

    if not isinstance(schema, dict):
        raise ValueError(
            f"Contract schema must be a mapping: "
            f"{contract_path}"
        )

    fields = schema.get("fields")

    if not isinstance(fields, list):
        raise ValueError(
            f"Contract schema.fields must be a list: "
            f"{contract_path}"
        )

    return contract


def load_contracts(
    contracts_directory: Path,
) -> list[Contract]:
    contracts_directory = Path(
        contracts_directory
    )

    if not contracts_directory.exists():
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

    contract_versions: set[
        tuple[str, int]
    ] = set()

    topics: set[str] = set()

    for contract_path in contract_paths:
        contract = load_contract(
            contract_path
        )

        name = contract["name"]
        version = contract["version"]
        topic = contract["topic"]

        contract_version = (
            name,
            version,
        )

        if contract_version in contract_versions:
            raise ValueError(
                "Duplicate contract version: "
                f"{name} v{version}"
            )

        if topic in topics:
            raise ValueError(
                f"Duplicate contract topic: "
                f"{topic}"
            )

        contract_versions.add(
            contract_version
        )

        topics.add(
            topic
        )

        contracts.append(
            contract
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
            key=lambda contract: (
                contract["version"]
            ),
        )

    for contract in matching_contracts:
        if contract["version"] == version:
            return contract

    raise ValueError(
        f"Contract not found: "
        f"{name} v{version}"
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
        f"Contract not found for topic: "
        f"{topic}"
    )