import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any, Callable
from uuid import uuid4

from confluent_kafka import Message, Producer
from dotenv import load_dotenv

from contract_registry import (
    load_contracts,
    map_contracts_by_name,
)
from validator import validate_event


EventBuilder = Callable[[str], dict[str, Any]]


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
        print(f"Delivery failed: {error}")
        return

    print(
        "Event delivered: "
        f"topic={message.topic()}, "
        f"partition={message.partition()}, "
        f"offset={message.offset()}"
    )


def main() -> None:
    load_dotenv()

    contracts_directory = os.getenv(
        "CONTRACTS_DIR",
        "contracts",
    )

    contracts = load_contracts(
        contracts_directory
    )

    contracts_by_name = map_contracts_by_name(
        contracts
    )

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--event",
        choices=sorted(contracts_by_name),
        default="order_created",
        help="Event contract name",
    )

    parser.add_argument(
        "--order-id",
        default="ord_1001",
        help="Order identifier",
    )

    parser.add_argument(
        "--invalid",
        action="store_true",
        help="Send an intentionally invalid event",
    )

    args = parser.parse_args()

    contract = contracts_by_name[args.event]

    builder = EVENT_BUILDERS.get(args.event)

    if builder is None:
        raise ValueError(
            f"No demo event builder for: {args.event}"
        )

    event = builder(args.order_id)

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

    key_fields = contract.get("key", [])

    if not key_fields:
        raise ValueError(
            f"Contract '{args.event}' has no key"
        )

    message_key = "|".join(
        str(event[field_name])
        for field_name in key_fields
    )

    producer = Producer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            "client.id": "orders-demo-producer",
            "enable.idempotence": True,
        }
    )

    producer.produce(
        topic=contract["topic"],
        key=message_key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
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