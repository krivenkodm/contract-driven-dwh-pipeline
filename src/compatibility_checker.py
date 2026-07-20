import os
from collections import defaultdict
from pathlib import Path
from typing import Any

from contract_registry import load_contracts


Contract = dict[str, Any]


def fields_by_name(
    contract: Contract,
) -> dict[str, dict[str, Any]]:
    fields = contract["schema"]["fields"]

    return {
        field["name"]: field
        for field in fields
    }


def check_backward_compatibility(
    old_contract: Contract,
    new_contract: Contract,
) -> list[str]:
    errors: list[str] = []

    contract_name = old_contract["name"]
    old_version = old_contract["version"]
    new_version = new_contract["version"]

    if old_contract["name"] != new_contract["name"]:
        errors.append(
            "Contracts have different names: "
            f"{old_contract['name']} and "
            f"{new_contract['name']}"
        )

        return errors

    old_key = old_contract.get("key", [])
    new_key = new_contract.get("key", [])

    if old_key != new_key:
        errors.append(
            f"{contract_name} v{old_version} -> "
            f"v{new_version}: key changed from "
            f"{old_key} to {new_key}"
        )

    old_fields = fields_by_name(old_contract)
    new_fields = fields_by_name(new_contract)

    for field_name, old_field in old_fields.items():
        new_field = new_fields.get(field_name)

        if new_field is None:
            errors.append(
                f"{contract_name} v{old_version} -> "
                f"v{new_version}: field removed: "
                f"{field_name}"
            )

            continue

        old_type = old_field["type"]
        new_type = new_field["type"]

        if old_type != new_type:
            errors.append(
                f"{contract_name} v{old_version} -> "
                f"v{new_version}: field {field_name} "
                f"changed type from {old_type} "
                f"to {new_type}"
            )

        old_nullable = old_field.get(
            "nullable",
            True,
        )

        new_nullable = new_field.get(
            "nullable",
            True,
        )

        if old_nullable and not new_nullable:
            errors.append(
                f"{contract_name} v{old_version} -> "
                f"v{new_version}: nullable field "
                f"{field_name} became required"
            )

    for field_name, new_field in new_fields.items():
        if field_name in old_fields:
            continue

        nullable = new_field.get(
            "nullable",
            True,
        )

        if not nullable:
            errors.append(
                f"{contract_name} v{old_version} -> "
                f"v{new_version}: new field "
                f"{field_name} is required"
            )

    return errors


def check_all_contracts(
    contracts: list[Contract],
) -> list[str]:
    errors: list[str] = []

    contracts_by_name: dict[
        str,
        list[Contract],
    ] = defaultdict(list)

    topics: dict[str, tuple[str, int]] = {}
    name_versions: set[tuple[str, int]] = set()

    for contract in contracts:
        name = contract["name"]
        version = contract["version"]
        topic = contract["topic"]

        name_version = (
            name,
            version,
        )

        if name_version in name_versions:
            errors.append(
                "Duplicate contract version: "
                f"{name} v{version}"
            )
        else:
            name_versions.add(name_version)

        if topic in topics:
            previous_name, previous_version = topics[
                topic
            ]

            errors.append(
                f"Duplicate topic {topic}: "
                f"{previous_name} v{previous_version} "
                f"and {name} v{version}"
            )
        else:
            topics[topic] = (
                name,
                version,
            )

        contracts_by_name[name].append(
            contract
        )

    for name, versions in contracts_by_name.items():
        ordered_versions = sorted(
            versions,
            key=lambda contract: contract["version"],
        )

        for old_contract, new_contract in zip(
            ordered_versions,
            ordered_versions[1:],
        ):
            old_version = old_contract["version"]
            new_version = new_contract["version"]

            if new_version <= old_version:
                errors.append(
                    f"{name}: invalid version order "
                    f"{old_version} -> {new_version}"
                )

                continue

            compatibility_mode = (
                new_contract
                .get("compatibility", {})
                .get("mode", "backward")
            )

            if compatibility_mode != "backward":
                errors.append(
                    f"{name} v{new_version}: "
                    "unsupported compatibility mode: "
                    f"{compatibility_mode}"
                )

                continue

            errors.extend(
                check_backward_compatibility(
                    old_contract=old_contract,
                    new_contract=new_contract,
                )
            )

    return errors


def main() -> None:
    contracts_directory = Path(
        os.getenv(
            "CONTRACTS_DIR",
            "contracts",
        )
    )

    contracts = load_contracts(
        contracts_directory
    )

    errors = check_all_contracts(
        contracts
    )

    if errors:
        print("Contract compatibility check failed:")

        for error in errors:
            print(f"  - {error}")

        raise SystemExit(1)

    versions_by_name: dict[
        str,
        list[int],
    ] = defaultdict(list)

    for contract in contracts:
        versions_by_name[
            contract["name"]
        ].append(
            contract["version"]
        )

    print("Contract compatibility check passed")

    for name in sorted(versions_by_name):
        versions = sorted(
            versions_by_name[name]
        )

        formatted_versions = ", ".join(
            f"v{version}"
            for version in versions
        )

        print(
            f"  - {name}: "
            f"{formatted_versions}"
        )


if __name__ == "__main__":
    main()