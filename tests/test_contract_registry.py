from pathlib import Path

from contract_registry import (
    load_contracts,
    map_contracts_by_topic,
)


CONTRACTS_DIR = (
    Path(__file__).resolve().parents[1]
    / "contracts"
)


def test_all_contracts_are_loaded() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    assert len(contracts) == 3


def test_contract_topics_are_loaded() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    topics = {
        contract["topic"]
        for contract in contracts
    }

    assert topics == {
        "ecommerce.order_created.v1",
        "ecommerce.order_paid.v1",
        "ecommerce.order_cancelled.v1",
    }


def test_contracts_are_mapped_by_topic() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    contracts_by_topic = map_contracts_by_topic(
        contracts
    )

    assert set(contracts_by_topic) == {
        "ecommerce.order_created.v1",
        "ecommerce.order_paid.v1",
        "ecommerce.order_cancelled.v1",
    }

    assert (
        contracts_by_topic[
            "ecommerce.order_created.v1"
        ]["topic"]
        == "ecommerce.order_created.v1"
    )


def test_every_contract_has_fields() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    for contract in contracts:
        assert "schema" in contract
        assert "fields" in contract["schema"]

        fields = contract["schema"]["fields"]

        assert fields
        assert isinstance(fields, list)


def test_every_contract_has_unique_topic() -> None:
    contracts = load_contracts(CONTRACTS_DIR)

    topics = [
        contract["topic"]
        for contract in contracts
    ]

    assert len(topics) == len(set(topics))