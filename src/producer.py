import argparse
import json
import os
import struct
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

from confluent_kafka import Message, Producer
from dotenv import load_dotenv

from avro_codec import serialize_avro_event
from contract_registry import (
    get_contract,
    load_contracts,
)
from validator import validate_event


Contract = dict[str, Any]
EventBuilder = Callable[[str], dict[str, Any]]

SUPPORTED_SERIALIZATIONS = ("json", "avro")
CORRUPT_SCHEMA_ID = 2_147_483_647


def current_timestamp() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def build_order_created(
    order_id: str,
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "customer_id": "cust_777",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": current_timestamp(),
    }


def build_order_paid(
    order_id: str,
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "payment_id": f"pay_{uuid4().hex[:10]}",
        "paid_amount": 1500.50,
        "currency": "RUB",
        "paid_at": current_timestamp(),
    }


def build_order_cancelled(
    order_id: str,
) -> dict[str, Any]:
    return {
        "order_id": order_id,
        "cancellation_id": (
            f"cancel_{uuid4().hex[:10]}"
        ),
        "cancellation_reason": (
            "customer_request"
        ),
        "cancelled_at": current_timestamp(),
    }


EVENT_BUILDERS: dict[str, EventBuilder] = {
    "order_created": build_order_created,
    "order_paid": build_order_paid,
    "order_cancelled": build_order_cancelled,
}


def get_contract_field_names(
    contract: Contract,
) -> set[str]:
    return {
        field["name"]
        for field in contract["schema"]["fields"]
    }


def get_available_versions(
    contracts: list[Contract],
    event_name: str,
) -> list[int]:
    return sorted(
        contract["version"]
        for contract in contracts
        if contract["name"] == event_name
    )


def add_version_specific_fields(
    event_name: str,
    event: dict[str, Any],
    contract: Contract,
    source_channel: str | None,
) -> None:
    contract_fields = get_contract_field_names(
        contract
    )

    if source_channel is None:
        return

    if event_name != "order_created":
        raise ValueError(
            "--source-channel can be used only "
            "with order_created"
        )

    if "source_channel" not in contract_fields:
        raise ValueError(
            f"Contract {event_name} "
            f"v{contract['version']} does not support "
            "source_channel"
        )

    event["source_channel"] = source_channel


def make_event_invalid(
    event_name: str,
    event: dict[str, Any],
) -> None:
    event["unexpected_field"] = "test"

    if event_name == "order_created":
        event["amount"] = "not-a-number"

    elif event_name == "order_paid":
        event["paid_amount"] = "not-a-number"

    elif event_name == "order_cancelled":
        event["cancelled_at"] = "not-a-date"


def delivery_callback(
    error: Exception | None,
    message: Message,
) -> None:
    if error is not None:
        print(
            f"Delivery failed: {error}"
        )
        return

    print(
        "Event delivered: "
        f"topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )


def _environment_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)

    if value is None:
        return default

    return value.strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def main() -> None:
    load_dotenv()

    contracts_directory = Path(
        os.getenv(
            "CONTRACTS_DIR",
            "contracts",
        )
    )

    contracts = load_contracts(
        contracts_directory
    )

    event_names = sorted(
        {
            contract["name"]
            for contract in contracts
        }
    )

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--event",
        choices=event_names,
        default="order_created",
        help="Event contract name",
    )

    parser.add_argument(
        "--version",
        type=int,
        default=None,
        help=(
            "Contract version. "
            "The latest version is used by default"
        ),
    )

    parser.add_argument(
        "--order-id",
        default="ord_1001",
        help="Order identifier",
    )

    parser.add_argument(
        "--source-channel",
        default=None,
        help=(
            "Source channel for order_created v2"
        ),
    )

    parser.add_argument(
        "--invalid",
        action="store_true",
        help="Send an intentionally invalid event",
    )

    parser.add_argument(
        "--serialization",
        choices=SUPPORTED_SERIALIZATIONS,
        default=os.getenv(
            "EVENT_SERIALIZATION",
            "json",
        ).lower(),
        help=(
            "Wire format. EVENT_SERIALIZATION is used "
            "when the argument is omitted"
        ),
    )

    parser.add_argument(
        "--corrupt-avro",
        action="store_true",
        help=(
            "Send an Avro frame with an unknown schema ID "
            "for DLQ testing"
        ),
    )

    args = parser.parse_args()

    if args.invalid and args.serialization == "avro":
        raise ValueError(
            "--invalid is available only for JSON because "
            "Avro rejects schema-invalid values before publish; "
            "use --corrupt-avro to test the Avro DLQ path"
        )

    if args.corrupt_avro and args.serialization != "avro":
        raise ValueError(
            "--corrupt-avro requires --serialization avro"
        )

    if args.invalid and args.corrupt_avro:
        raise ValueError(
            "--invalid and --corrupt-avro are mutually exclusive"
        )

    try:
        contract = get_contract(
            contracts=contracts,
            name=args.event,
            version=args.version,
        )

    except ValueError as error:
        available_versions = get_available_versions(
            contracts=contracts,
            event_name=args.event,
        )

        raise ValueError(
            f"Contract not found: "
            f"{args.event} v{args.version}. "
            f"Available versions: "
            f"{available_versions}"
        ) from error

    builder = EVENT_BUILDERS.get(
        args.event
    )

    if builder is None:
        raise ValueError(
            "No demo event builder for: "
            f"{args.event}"
        )

    event = builder(
        args.order_id
    )

    add_version_specific_fields(
        event_name=args.event,
        event=event,
        contract=contract,
        source_channel=args.source_channel,
    )

    if args.invalid:
        make_event_invalid(
            event_name=args.event,
            event=event,
        )

    else:
        errors = validate_event(
            event=event,
            contract=contract,
        )

        if errors:
            raise ValueError(
                "Generated event is invalid: "
                + "; ".join(errors)
            )

    key_fields = contract.get(
        "key",
        [],
    )

    if not key_fields:
        raise ValueError(
            f"Contract '{args.event}' has no key"
        )

    missing_key_fields = [
        field_name
        for field_name in key_fields
        if field_name not in event
    ]

    if missing_key_fields:
        raise ValueError(
            "Event does not contain key fields: "
            + ", ".join(missing_key_fields)
        )

    message_key = "|".join(
        str(event[field_name])
        for field_name in key_fields
    )

    producer = Producer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:19092",
            ),
            "client.id": "orders-demo-producer",
            "enable.idempotence": True,
        }
    )

    print(
        "Producing event: "
        f"name={contract['name']}, "
        f"version={contract['version']}, "
        f"topic={contract['topic']}, "
        f"serialization={args.serialization}"
    )

    print(
        json.dumps(
            event,
            ensure_ascii=False,
            indent=2,
        )
    )

    if args.serialization == "avro":
        if args.corrupt_avro:
            message_value = struct.pack(
                ">bI",
                0,
                CORRUPT_SCHEMA_ID,
            ) + b"\x00"
        else:
            message_value = serialize_avro_event(
                event=event,
                contract=contract,
                registry_url=os.getenv(
                    "SCHEMA_REGISTRY_URL",
                    "http://localhost:18081",
                ),
                auto_register_schemas=_environment_flag(
                    "SCHEMA_REGISTRY_AUTO_REGISTER",
                ),
            )
    else:
        message_value = json.dumps(
            event,
            ensure_ascii=False,
        ).encode("utf-8")

    producer.produce(
        topic=contract["topic"],
        key=message_key.encode("utf-8"),
        value=message_value,
        callback=delivery_callback,
    )

    remaining_messages = producer.flush(
        timeout=10
    )

    if remaining_messages > 0:
        raise RuntimeError(
            f"{remaining_messages} messages "
            "were not delivered"
        )


if __name__ == "__main__":
    main()
