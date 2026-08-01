from typing import Any

from compatibility_checker import (
    check_backward_compatibility,
)


Contract = dict[str, Any]


def make_contract(
    version: int,
    fields: list[dict[str, Any]],
    allow_extra_fields: bool = False,
    unique_key: list[str] | None = None,
    not_null: list[str] | None = None,
) -> Contract:
    return {
        "name": "test_event",
        "version": version,
        "topic": f"test.event.v{version}",
        "owner": "test-team",
        "key": [
            "event_id",
        ],
        "compatibility": {
            "mode": "backward",
        },
        "schema": {
            "allow_extra_fields": allow_extra_fields,
            "fields": fields,
        },
        "quality": {
            "unique_key": (
                unique_key
                if unique_key is not None
                else ["event_id"]
            ),
            "not_null": (
                not_null
                if not_null is not None
                else ["event_id"]
            ),
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


def test_required_field_with_default_can_be_added() -> None:
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
                "name": "source",
                "type": "string",
                "nullable": False,
                "default": "unknown",
            },
        ],
    )

    assert check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    ) == []


def test_enum_can_be_expanded() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
                "enum": ["a", "b"],
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
                "enum": ["a", "b", "c"],
            },
        ],
    )

    assert check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    ) == []


def test_enum_cannot_be_narrowed() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
                "enum": ["a", "b"],
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
                "enum": ["a"],
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "removed enum values" in error
        for error in errors
    )


def test_extra_fields_cannot_become_forbidden() -> None:
    fields = [
        {
            "name": "event_id",
            "type": "string",
            "nullable": False,
        },
    ]

    old_contract = make_contract(
        version=1,
        fields=fields,
        allow_extra_fields=True,
    )

    new_contract = make_contract(
        version=2,
        fields=fields,
        allow_extra_fields=False,
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "extra fields became forbidden" in error
        for error in errors
    )


def test_topic_namespace_cannot_change() -> None:
    fields = [
        {
            "name": "event_id",
            "type": "string",
            "nullable": False,
        },
    ]

    old_contract = make_contract(
        version=1,
        fields=fields,
    )
    new_contract = make_contract(
        version=2,
        fields=fields,
    )
    new_contract["topic"] = "other.event.v2"

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "topic namespace changed" in error
        for error in errors
    )


def test_existing_default_cannot_be_removed() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": False,
                "default": "unknown",
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
        "changed or removed its default" in error
        for error in errors
    )


def test_existing_null_default_cannot_be_removed() -> None:
    old_contract = make_contract(
        version=1,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": True,
                "default": None,
            },
        ],
    )

    new_contract = make_contract(
        version=2,
        fields=[
            {
                "name": "event_id",
                "type": "string",
                "nullable": True,
            },
        ],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "changed or removed its default" in error
        for error in errors
    )


def test_quality_rules_cannot_be_strengthened() -> None:
    fields = [
        {
            "name": "event_id",
            "type": "string",
            "nullable": False,
        },
        {
            "name": "source",
            "type": "string",
            "nullable": True,
        },
    ]

    old_contract = make_contract(
        version=1,
        fields=fields,
        not_null=["event_id"],
    )

    new_contract = make_contract(
        version=2,
        fields=fields,
        not_null=["event_id", "source"],
    )

    errors = check_backward_compatibility(
        old_contract=old_contract,
        new_contract=new_contract,
    )

    assert any(
        "quality.not_null added" in error
        for error in errors
    )
