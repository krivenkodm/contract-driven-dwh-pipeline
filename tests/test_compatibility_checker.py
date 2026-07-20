from typing import Any

from compatibility_checker import (
    check_backward_compatibility,
)


Contract = dict[str, Any]


def make_contract(
    version: int,
    fields: list[dict[str, Any]],
) -> Contract:
    return {
        "name": "test_event",
        "version": version,
        "topic": f"test.event.v{version}",
        "key": [
            "event_id",
        ],
        "compatibility": {
            "mode": "backward",
        },
        "schema": {
            "fields": fields,
        },
    }


def test_nullable_field_can_be_added() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
            {
                "name": "source_channel",
                "type": "string",
                "nullable": True,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert errors == []


def test_required_field_cannot_be_added() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
            {
                "name": "source_channel",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "new field source_channel is required"
        in error
        for error in errors
    )


def test_existing_field_cannot_be_removed() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
            {
                "name": "source_channel",
                "type": "string",
                "nullable": True,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "field removed: source_channel"
        in error
        for error in errors
    )


def test_existing_field_type_cannot_change() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "timestamp",
                "nullable": False,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "changed type"
        in error
        for error in errors
    )


def test_nullable_field_cannot_become_required() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": True,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "became required"
        in error
        for error in errors
    )