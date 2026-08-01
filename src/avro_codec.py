from typing import Any

from confluent_kafka.schema_registry import (
    SchemaRegistryClient,
    record_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import (
    AvroDeserializer,
    AvroSerializer,
)
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
)

from avro_schema import (
    avro_schema_json,
    prepare_event_for_avro,
)


Contract = dict[str, Any]

CONFLUENT_MAGIC_BYTE = 0


def is_confluent_avro(payload: bytes | None) -> bool:
    return bool(
        payload
        and payload[0] == CONFLUENT_MAGIC_BYTE
    )


def create_schema_registry_client(
    registry_url: str,
) -> SchemaRegistryClient:
    if not registry_url:
        raise ValueError("Schema Registry URL must not be empty")

    return SchemaRegistryClient({"url": registry_url})


def create_avro_deserializer(
    registry_url: str,
) -> AvroDeserializer:
    return AvroDeserializer(
        create_schema_registry_client(registry_url)
    )


def serialize_avro_event(
    event: dict[str, Any],
    contract: Contract,
    registry_url: str,
    auto_register_schemas: bool = False,
) -> bytes:
    client = create_schema_registry_client(registry_url)
    serializer = AvroSerializer(
        schema_registry_client=client,
        schema_str=avro_schema_json(contract),
        conf={
            "auto.register.schemas": auto_register_schemas,
            "subject.name.strategy": (
                record_subject_name_strategy
            ),
            "validate.strict": False,
            "validate.strict.allow.default": True,
        },
    )
    context = SerializationContext(
        contract["topic"],
        MessageField.VALUE,
    )
    serialized = serializer(
        prepare_event_for_avro(event, contract),
        context,
    )

    if serialized is None:
        raise ValueError("Avro serializer returned a null payload")

    return serialized
