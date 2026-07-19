from pathlib import Path
from typing import Any

import yaml


def load_contract(path: str | Path) -> dict[str, Any]:
    contract_path = Path(path)

    if not contract_path.exists():
        raise FileNotFoundError(
            f"Contract file does not exist: {contract_path}"
        )

    with contract_path.open("r", encoding="utf-8") as file:
        contract = yaml.safe_load(file)

    if not isinstance(contract, dict):
        raise ValueError("Contract must be a YAML object")

    required_sections = {"name", "version", "topic", "schema"}

    missing_sections = required_sections - contract.keys()
    if missing_sections:
        raise ValueError(
            f"Contract is missing sections: {sorted(missing_sections)}"
        )

    fields = contract.get("schema", {}).get("fields")

    if not isinstance(fields, list) or not fields:
        raise ValueError("Contract schema.fields must be a non-empty list")

    return contract