from typing import Any

import pytest

import consumer
from consumer import parse_message, process_message


class FakeMessage:
    def __init__(self, value: bytes | None) -> None:
        self._value = value

    def value(self) -> bytes | None:
        return self._value

    def topic(self) -> str:
        return "test.events.v1"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 42


class FakeWriter:
    def __init__(self) -> None:
        self.dead_letters: list[dict[str, Any]] = []
        self.valid_events: list[dict[str, Any]] = []

    def write_dead_letter(self, **values: Any) -> bool:
        self.dead_letters.append(values)
        return True

    def write_valid_event(self, **values: Any) -> bool:
        self.valid_events.append(values)
        return True


@pytest.mark.parametrize(
    ("payload", "expected_error"),
    [
        (None, "must not be null"),
        (b"not-json", "not valid JSON"),
        (b'{"amount": NaN}', "Non-standard JSON constant"),
    ],
)
def test_invalid_message_payload_is_rejected(
    payload: bytes | None,
    expected_error: str,
) -> None:
    parsed, errors = parse_message(
        FakeMessage(payload)  # type: ignore[arg-type]
    )

    assert parsed is None
    assert any(
        expected_error in error
        for error in errors
    )


def test_unexpected_validator_error_is_saved_to_dead_letter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fail_validation(**_: Any) -> list[str]:
        raise RuntimeError("validator failed")

    monkeypatch.setattr(
        consumer,
        "validate_event",
        fail_validation,
    )

    writer = FakeWriter()
    contract = {
        "name": "test_event",
        "schema": {
            "fields": [
                {
                    "name": "event_id",
                    "type": "string",
                    "nullable": False,
                }
            ]
        },
    }

    process_message(
        message=FakeMessage(  # type: ignore[arg-type]
            b'{"event_id": "event_1"}'
        ),
        contract=contract,
        writer=writer,  # type: ignore[arg-type]
    )

    assert writer.valid_events == []
    assert len(writer.dead_letters) == 1
    assert (
        "RuntimeError: validator failed"
        in writer.dead_letters[0]["error_message"]
    )
    assert writer.dead_letters[0]["contract"] is contract
    assert (
        writer.dead_letters[0]["raw_payload"]
        == b'{"event_id": "event_1"}'
    )


def test_tombstone_is_saved_with_null_raw_payload() -> None:
    writer = FakeWriter()
    contract = {
        "name": "test_event",
        "version": 1,
        "schema": {
            "fields": [],
        },
    }

    process_message(
        message=FakeMessage(None),  # type: ignore[arg-type]
        contract=contract,
        writer=writer,  # type: ignore[arg-type]
    )

    assert len(writer.dead_letters) == 1
    assert writer.dead_letters[0]["raw_payload"] is None
    assert writer.dead_letters[0]["payload"] is None
