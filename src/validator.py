import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any


DECIMAL_PATTERN = re.compile(r"decimal\((\d+),(\d+)\)")


def _validate_decimal(
    value: Any,
    contract_type: str,
    field_name: str,
) -> list[str]:
    if isinstance(value, bool):
        return [f"Field '{field_name}' must be decimal"]

    try:
        decimal_value = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return [f"Field '{field_name}' must be decimal"]

    match = DECIMAL_PATTERN.fullmatch(contract_type)

    if not match:
        return [f"Unsupported decimal type '{contract_type}'"]

    precision = int(match.group(1))
    allowed_scale = int(match.group(2))

    _, digits, exponent = decimal_value.as_tuple()

    actual_scale = max(-exponent, 0)
    actual_precision = len(digits)

    errors: list[str] = []

    if actual_precision > precision:
        errors.append(
            f"Field '{field_name}' exceeds precision {precision}"
        )

    if actual_scale > allowed_scale:
        errors.append(
            f"Field '{field_name}' exceeds scale {allowed_scale}"
        )

    return errors


def _validate_timestamp(value: Any, field_name: str) -> list[str]:
    if not isinstance(value, str):
        return [f"Field '{field_name}' must be an ISO timestamp string"]

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return [f"Field '{field_name}' must be a valid ISO timestamp"]

    return []


def validate_event(
    event: dict[str, Any],
    contract: dict[str, Any],
) -> list[str]:
    if not isinstance(event, dict):
        return ["Event must be a JSON object"]

    errors: list[str] = []

    fields = contract["schema"]["fields"]
    expected_fields = {field["name"] for field in fields}

    allow_extra_fields = contract["schema"].get(
        "allow_extra_fields",
        False,
    )

    if not allow_extra_fields:
        extra_fields = set(event) - expected_fields

        for field_name in sorted(extra_fields):
            errors.append(f"Unexpected field '{field_name}'")

    for field in fields:
        field_name = field["name"]
        field_type = field["type"]
        nullable = field.get("nullable", True)

        value = event.get(field_name)

        if value is None:
            if not nullable:
                errors.append(f"Field '{field_name}' is required")
            continue

        if field_type == "string":
            if not isinstance(value, str):
                errors.append(f"Field '{field_name}' must be string")

        elif field_type.startswith("decimal"):
            errors.extend(
                _validate_decimal(
                    value=value,
                    contract_type=field_type,
                    field_name=field_name,
                )
            )

        elif field_type == "timestamp":
            errors.extend(
                _validate_timestamp(
                    value=value,
                    field_name=field_name,
                )
            )

        else:
            errors.append(
                f"Unsupported type '{field_type}' "
                f"for field '{field_name}'"
            )

        allowed_values = field.get("enum")

        if allowed_values is not None and value not in allowed_values:
            errors.append(
                f"Field '{field_name}' has value '{value}'. "
                f"Allowed values: {allowed_values}"
            )

    return errors