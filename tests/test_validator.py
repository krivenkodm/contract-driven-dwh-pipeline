from pathlib import Path
from typing import Any

import pytest

from contract_registry import load_contracts
from validator import validate_event


CONTRACTS_DIR = (
    Path(__file__).resolve().parents[1]
    / "contracts"
)


@pytest.fixture
def order_created_contract() -> dict[str, Any]:
    contracts = load_contracts(CONTRACTS_DIR)

    return next(
        contract
        for contract in contracts
        if contract["name"] == "order_created"
    )


def test_valid_event_has_no_errors(
    order_created_contract: dict[str, Any],
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-07-19T12:00:00Z",
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert errors == []


def test_required_field_is_checked(
    order_created_contract: dict[str, Any],
) -> None:
    event = {
        "customer_id": "customer_1001",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-07-19T12:00:00Z",
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert any(
        "order_id" in error
        for error in errors
    )


def test_unexpected_field_is_checked(
    order_created_contract: dict[str, Any],
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-07-19T12:00:00Z",
        "unexpected_field": "value",
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert any(
        "unexpected_field" in error
        for error in errors
    )