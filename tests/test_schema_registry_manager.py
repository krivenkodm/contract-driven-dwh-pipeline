from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka.schema_registry import (
    ConfigCompatibilityLevel,
    ServerConfig,
)

from contract_registry import load_contracts
from schema_registry_manager import (
    check_contract_schemas,
    register_contract_schemas,
    wait_for_schema_registry,
)


class FakeRegistryClient:
    def __init__(self) -> None:
        self.configs: dict[str, ServerConfig] = {}
        self.schemas: dict[str, list[Any]] = {}
        self.compatible = True

    def get_subjects(self) -> list[str]:
        return sorted(self.schemas)

    def set_config(
        self,
        subject_name: str | None = None,
        config: ServerConfig | None = None,
    ) -> ServerConfig:
        assert subject_name is not None
        assert config is not None
        self.configs[subject_name] = config
        return config

    def get_config(
        self,
        subject_name: str | None = None,
    ) -> ServerConfig:
        assert subject_name is not None
        return self.configs[subject_name]

    def get_versions(self, subject_name: str) -> list[int]:
        return list(
            range(1, len(self.schemas[subject_name]) + 1)
        )

    def test_compatibility(
        self,
        subject_name: str,
        schema: Any,
        version: int | str = "latest",
    ) -> bool:
        assert subject_name in self.schemas
        assert version == "latest"
        return self.compatible

    def register_schema_full_response(
        self,
        subject_name: str,
        schema: Any,
    ) -> Any:
        subject_schemas = self.schemas.setdefault(
            subject_name,
            [],
        )

        for version, existing in enumerate(
            subject_schemas,
            start=1,
        ):
            if existing == schema:
                return SimpleNamespace(
                    schema_id=version,
                    version=version,
                )

        subject_schemas.append(schema)
        version = len(subject_schemas)
        return SimpleNamespace(
            schema_id=version,
            version=version,
        )

    def lookup_schema(
        self,
        subject_name: str,
        schema: Any,
    ) -> Any:
        for version, existing in enumerate(
            self.schemas.get(subject_name, []),
            start=1,
        ):
            if existing == schema:
                return SimpleNamespace(
                    schema_id=version,
                    version=version,
                )

        raise ValueError("schema not found")


def test_all_versions_are_registered_under_business_subject() -> None:
    client = FakeRegistryClient()
    contracts = load_contracts(Path("contracts"))

    registrations = register_contract_schemas(
        client,
        contracts,
    )

    created_subject = "io.contract_dwh.events.order_created"
    assert len(client.schemas[created_subject]) == 2
    assert client.configs[created_subject].compatibility == (
        ConfigCompatibilityLevel.BACKWARD
    )
    assert len(registrations) == len(contracts)

    checked = check_contract_schemas(client, contracts)
    assert checked == registrations


def test_incompatible_registry_evolution_is_rejected() -> None:
    client = FakeRegistryClient()
    client.compatible = False
    contracts = [
        contract
        for contract in load_contracts(Path("contracts"))
        if contract["name"] == "order_created"
    ]

    with pytest.raises(
        ValueError,
        match="Registry rejected compatibility",
    ):
        register_contract_schemas(client, contracts)


def test_wait_for_registry_reports_unavailable_service() -> None:
    class UnavailableRegistry(FakeRegistryClient):
        def get_subjects(self) -> list[str]:
            raise ConnectionError("not ready")

    with pytest.raises(
        RuntimeError,
        match="did not become ready",
    ):
        wait_for_schema_registry(
            UnavailableRegistry(),
            attempts=2,
            delay_seconds=0,
        )


def test_unmanaged_registry_version_is_rejected() -> None:
    client = FakeRegistryClient()
    contracts = load_contracts(Path("contracts"))
    register_contract_schemas(client, contracts)
    client.schemas[
        "io.contract_dwh.events.order_created"
    ].append(object())

    with pytest.raises(
        ValueError,
        match="registry versions",
    ):
        check_contract_schemas(client, contracts)
