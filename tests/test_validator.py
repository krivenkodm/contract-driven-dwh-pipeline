from pathlib import Path
from typing import Any

import pytest

from contract_registry import get_contract, load_contracts
from validator import validate_event


CONTRACTS_DIR = (
    Path(__file__).resolve().parents[1]
    / "contracts"
)


@pytest.fixture(
    params=[1, 2],
    ids=["order_created_v1", "order_created_v2"],
)
def order_created_contract(
    request: pytest.FixtureRequest,
) -> dict[str, Any]:
    contracts = load_contracts(CONTRACTS_DIR)

    return get_contract(
        contracts=contracts,
        name="order_created",
        version=request.param,
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


def test_currency_enum_is_checked_in_every_version(
    order_created_contract: dict[str, Any],
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": 1500.50,
        "currency": "BTC",
        "created_at": "2026-07-19T12:00:00Z",
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert any(
        "Allowed values" in error
        for error in errors
    )


@pytest.mark.parametrize(
    "invalid_amount",
    [
        10_000_000_000,
        1e20,
        float("nan"),
        float("inf"),
        -float("inf"),
        "1500.50",
        True,
    ],
)
def test_invalid_decimal_values_are_rejected(
    order_created_contract: dict[str, Any],
    invalid_amount: Any,
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": invalid_amount,
        "currency": "RUB",
        "created_at": "2026-07-19T12:00:00Z",
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert any(
        "amount" in error
        for error in errors
    )


def test_decimal_boundary_is_valid(
    order_created_contract: dict[str, Any],
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": 9_999_999_999.99,
        "currency": "RUB",
        "created_at": "2026-07-19T12:00:00Z",
    }

    assert validate_event(
        event=event,
        contract=order_created_contract,
    ) == []


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-07-19",
        "2026-07-19T12:00:00",
    ],
)
def test_timestamp_without_timezone_is_rejected(
    order_created_contract: dict[str, Any],
    timestamp: str,
) -> None:
    event = {
        "order_id": "ord_1001",
        "customer_id": "customer_1001",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": timestamp,
    }

    errors = validate_event(
        event=event,
        contract=order_created_contract,
    )

    assert any(
        "timezone" in error
        for error in errors
    )


def test_missing_required_field_with_default_is_valid() -> None:
    contract = {
        "schema": {
            "allow_extra_fields": False,
            "fields": [
                {
                    "name": "source",
                    "type": "string",
                    "nullable": False,
                    "default": "unknown",
                }
            ],
        }
    }

    assert validate_event(
        event={},
        contract=contract,
    ) == []
