from pathlib import Path
from typing import Any

from contract_loader import load_contract


def load_contracts(
    contracts_directory: str | Path,
) -> list[dict[str, Any]]:
    directory = Path(contracts_directory)

    if not directory.exists():
        raise FileNotFoundError(
            f"Contracts directory does not exist: {directory}"
        )

    contract_paths = sorted(directory.glob("*.yaml"))

    if not contract_paths:
        raise ValueError(
            f"No YAML contracts found in: {directory}"
        )

    contracts = [
        load_contract(contract_path)
        for contract_path in contract_paths
    ]

    contract_names: set[str] = set()
    contract_topics: set[str] = set()

    for contract in contracts:
        name = contract["name"]
        topic = contract["topic"]

        if name in contract_names:
            raise ValueError(
                f"Duplicate contract name: {name}"
            )

        if topic in contract_topics:
            raise ValueError(
                f"Duplicate contract topic: {topic}"
            )

        contract_names.add(name)
        contract_topics.add(topic)

    return contracts


def map_contracts_by_name(
    contracts: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        contract["name"]: contract
        for contract in contracts
    }


def map_contracts_by_topic(
    contracts: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        contract["topic"]: contract
        for contract in contracts
    }