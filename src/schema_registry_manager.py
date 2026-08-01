import argparse
import os
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Protocol

from confluent_kafka.schema_registry import (
    ConfigCompatibilityLevel,
    Schema,
    SchemaRegistryClient,
    ServerConfig,
)

from avro_schema import avro_schema_json, get_avro_subject
from contract_registry import load_contracts


Contract = dict[str, Any]


class RegistryClient(Protocol):
    def get_subjects(self) -> list[str]: ...

    def set_config(
        self,
        subject_name: str | None = None,
        config: ServerConfig | None = None,
    ) -> ServerConfig: ...

    def get_config(
        self,
        subject_name: str | None = None,
    ) -> ServerConfig: ...

    def get_versions(
        self,
        subject_name: str,
    ) -> list[int]: ...

    def test_compatibility(
        self,
        subject_name: str,
        schema: Schema,
        version: int | str = "latest",
    ) -> bool: ...

    def register_schema_full_response(
        self,
        subject_name: str,
        schema: Schema,
    ) -> Any: ...

    def lookup_schema(
        self,
        subject_name: str,
        schema: Schema,
    ) -> Any: ...


def wait_for_schema_registry(
    client: RegistryClient,
    attempts: int = 30,
    delay_seconds: float = 1.0,
) -> None:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            client.get_subjects()
            return
        except Exception as error:
            last_error = error

            if attempt < attempts:
                time.sleep(delay_seconds)

    raise RuntimeError(
        "Schema Registry did not become ready"
    ) from last_error


def _contracts_by_subject(
    contracts: list[Contract],
) -> dict[str, list[Contract]]:
    grouped: dict[str, list[Contract]] = defaultdict(list)

    for contract in contracts:
        grouped[get_avro_subject(contract)].append(contract)

    return {
        subject: sorted(
            subject_contracts,
            key=lambda contract: contract["version"],
        )
        for subject, subject_contracts in grouped.items()
    }


def _validate_registry_versions(
    client: RegistryClient,
    subject: str,
    expected_count: int,
) -> None:
    actual_versions = sorted(client.get_versions(subject))
    expected_versions = list(range(1, expected_count + 1))

    if actual_versions != expected_versions:
        raise ValueError(
            f"Subject {subject} has registry versions "
            f"{actual_versions}, expected {expected_versions}"
        )


def register_contract_schemas(
    client: RegistryClient,
    contracts: list[Contract],
) -> list[dict[str, Any]]:
    registrations: list[dict[str, Any]] = []
    backward_config = ServerConfig(
        compatibility=ConfigCompatibilityLevel.BACKWARD
    )

    for subject, subject_contracts in sorted(
        _contracts_by_subject(contracts).items()
    ):
        client.set_config(
            subject_name=subject,
            config=backward_config,
        )

        for index, contract in enumerate(subject_contracts):
            schema = Schema(avro_schema_json(contract), "AVRO")

            if (
                index > 0
                and not client.test_compatibility(
                    subject,
                    schema,
                    version="latest",
                )
            ):
                raise ValueError(
                    f"Registry rejected compatibility for "
                    f"{contract['name']} v{contract['version']}"
                )

            registered = client.register_schema_full_response(
                subject,
                schema,
            )
            looked_up = client.lookup_schema(subject, schema)
            registrations.append(
                {
                    "subject": subject,
                    "contract_name": contract["name"],
                    "contract_version": contract["version"],
                    "schema_id": registered.schema_id,
                    "registry_version": looked_up.version,
                }
            )

        _validate_registry_versions(
            client,
            subject,
            expected_count=len(subject_contracts),
        )

    return registrations


def check_contract_schemas(
    client: RegistryClient,
    contracts: list[Contract],
) -> list[dict[str, Any]]:
    registrations: list[dict[str, Any]] = []

    for subject, subject_contracts in sorted(
        _contracts_by_subject(contracts).items()
    ):
        config = client.get_config(subject)
        compatibility = (
            config.compatibility
            or config.compatibility_level
        )

        if compatibility != ConfigCompatibilityLevel.BACKWARD:
            raise ValueError(
                f"Subject {subject} compatibility is "
                f"{compatibility}, expected BACKWARD"
            )

        _validate_registry_versions(
            client,
            subject,
            expected_count=len(subject_contracts),
        )

        for contract in subject_contracts:
            schema = Schema(avro_schema_json(contract), "AVRO")
            registered = client.lookup_schema(subject, schema)
            registrations.append(
                {
                    "subject": subject,
                    "contract_name": contract["name"],
                    "contract_version": contract["version"],
                    "schema_id": registered.schema_id,
                    "registry_version": registered.version,
                }
            )

    return registrations


def _print_registrations(
    registrations: list[dict[str, Any]],
    action: str,
) -> None:
    for registration in registrations:
        print(
            f"{action}: "
            f"{registration['contract_name']} "
            f"v{registration['contract_version']} -> "
            f"subject={registration['subject']}, "
            f"registry_version="
            f"{registration['registry_version']}, "
            f"schema_id={registration['schema_id']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify schemas without registering changes",
    )
    parser.add_argument(
        "--wait-attempts",
        type=int,
        default=30,
    )
    parser.add_argument(
        "--wait-delay-seconds",
        type=float,
        default=1.0,
    )
    args = parser.parse_args()

    contracts = load_contracts(
        Path(os.getenv("CONTRACTS_DIR", "contracts"))
    )
    registry_url = os.getenv(
        "SCHEMA_REGISTRY_URL",
        "http://localhost:18081",
    )
    client = SchemaRegistryClient({"url": registry_url})
    wait_for_schema_registry(
        client,
        attempts=args.wait_attempts,
        delay_seconds=args.wait_delay_seconds,
    )

    if args.check:
        registrations = check_contract_schemas(
            client,
            contracts,
        )
        action = "Schema verified"
    else:
        registrations = register_contract_schemas(
            client,
            contracts,
        )
        action = "Schema registered"

    _print_registrations(registrations, action)


if __name__ == "__main__":
    main()
