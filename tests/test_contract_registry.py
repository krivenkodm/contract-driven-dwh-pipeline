from pathlib import Path

from contract_registry import (
    get_contract,
    get_contract_by_topic,
    load_contracts,
    map_contracts_by_topic,
)


CONTRACTS_DIR = Path("contracts")

EXPECTED_TOPICS = {
    "ecommerce.order_created.v1",
    "ecommerce.order_created.v2",
    "ecommerce.order_paid.v1",
    "ecommerce.order_cancelled.v1",
}


def test_all_contracts_are_loaded() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    assert len(contracts) == 4


def test_contract_topics_are_loaded() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    topics = {
        contract["topic"]
        for contract in contracts
    }

    assert topics == EXPECTED_TOPICS


def test_contracts_are_mapped_by_topic() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contracts_by_topic = map_contracts_by_topic(
        contracts
    )

    assert set(contracts_by_topic) == EXPECTED_TOPICS


def test_order_created_has_two_versions() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    versions = sorted(
        contract["version"]
        for contract in contracts
        if contract["name"] == "order_created"
    )

    assert versions == [1, 2]


def test_latest_contract_is_selected_by_default() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contract = get_contract(
        contracts=contracts,
        name="order_created",
    )

    assert contract["version"] == 2
    assert (
        contract["topic"]
        == "ecommerce.order_created.v2"
    )


def test_specific_contract_version_can_be_selected() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contract = get_contract(
        contracts=contracts,
        name="order_created",
        version=1,
    )

    assert contract["version"] == 1
    assert (
        contract["topic"]
        == "ecommerce.order_created.v1"
    )


def test_contract_can_be_found_by_topic() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contract = get_contract_by_topic(
        contracts=contracts,
        topic="ecommerce.order_created.v2",
    )

    assert contract["name"] == "order_created"
    assert contract["version"] == 2


def test_v1_does_not_contain_source_channel() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contract = get_contract(
        contracts=contracts,
        name="order_created",
        version=1,
    )

    field_names = {
        field["name"]
        for field in contract["schema"]["fields"]
    }

    assert "source_channel" not in field_names


def test_v2_contains_nullable_source_channel() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contract = get_contract(
        contracts=contracts,
        name="order_created",
        version=2,
    )

    fields_by_name = {
        field["name"]: field
        for field in contract["schema"]["fields"]
    }

    assert "source_channel" in fields_by_name
    assert (
        fields_by_name["source_channel"]["nullable"]
        is True
    )


def test_every_contract_has_fields() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    for contract in contracts:
        assert contract["schema"]["fields"]


def test_every_contract_has_unique_topic() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    topics = [
        contract["topic"]
        for contract in contracts
    ]

    assert len(topics) == len(set(topics))