from typing import Any

from psycopg.types.json import Jsonb

from dwh_writer import DwhWriter


class FakeCursor:
    rowcount = 1


class FakeConnection:
    def __init__(self) -> None:
        self.parameters: list[Any] | None = None
        self.committed = False

    def execute(
        self,
        _: Any,
        parameters: list[Any],
    ) -> FakeCursor:
        self.parameters = parameters
        return FakeCursor()

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        raise AssertionError("rollback was not expected")


def test_valid_event_uses_contract_provenance_and_default() -> None:
    connection = FakeConnection()
    writer = DwhWriter.__new__(DwhWriter)
    writer.connection = connection  # type: ignore[assignment]

    event = {
        "event_id": "event_1",
    }
    contract = {
        "name": "test_event",
        "version": 2,
        "schema": {
            "fields": [
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
        },
    }

    inserted = writer.write_valid_event(
        contract=contract,
        event=event,
        kafka_topic="test.test_event.v2",
        kafka_partition=0,
        kafka_offset=42,
    )

    assert inserted is True
    assert connection.committed is True
    assert connection.parameters is not None
    assert connection.parameters[:5] == [
        "test.test_event.v2",
        0,
        42,
        "test_event",
        2,
    ]

    original_payload = connection.parameters[5]
    assert isinstance(original_payload, Jsonb)
    assert original_payload.obj == event
    assert connection.parameters[6:] == [
        "event_1",
        "unknown",
    ]
