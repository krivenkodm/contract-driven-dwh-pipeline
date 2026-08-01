from pathlib import Path

import avro_codec
from avro_codec import (
    is_confluent_avro,
    serialize_avro_event,
)
from avro_schema import (
    avro_schema_json,
    get_avro_subject,
    normalize_avro_event,
)
from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry._sync.mock_schema_registry_client import (
    MockSchemaRegistryClient,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)
from contract_registry import get_contract, load_contracts


def test_registered_avro_event_round_trip(
    monkeypatch,
) -> None:
    contract = get_contract(
        load_contracts(Path("contracts")),
        "order_created",
        2,
    )
    registry = MockSchemaRegistryClient(
        {"url": "mock://stage-8"}
    )
    registry.register_schema(
        get_avro_subject(contract),
        Schema(avro_schema_json(contract), "AVRO"),
    )
    monkeypatch.setattr(
        avro_codec,
        "create_schema_registry_client",
        lambda _: registry,
    )
    event = {
        "order_id": "ord_avro_1",
        "customer_id": "customer_1",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-08-01T12:00:00Z",
    }

    payload = serialize_avro_event(
        event=event,
        contract=contract,
        registry_url="mock://stage-8",
    )

    assert is_confluent_avro(payload)

    deserializer = AvroDeserializer(registry)
    decoded = deserializer(
        payload,
        SerializationContext(
            contract["topic"],
            MessageField.VALUE,
        ),
    )

    assert isinstance(decoded, dict)
    normalized = normalize_avro_event(decoded)
    assert normalized["order_id"] == "ord_avro_1"
    assert normalized["source_channel"] is None


def test_json_is_not_detected_as_confluent_avro() -> None:
    assert is_confluent_avro(b'{"event_id":"1"}') is False
    assert is_confluent_avro(None) is False
