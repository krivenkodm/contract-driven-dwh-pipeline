import argparse
import json
import os
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Message, Producer
from dotenv import load_dotenv

from contract_loader import load_contract
from validator import validate_event


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


def build_event(invalid: bool = False) -> dict[str, Any]:
    event: dict[str, Any] = {
        "order_id": "ord_1001",
        "customer_id": "cust_777",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    if invalid:
        event["amount"] = "not-a-number"
        event["unexpected_field"] = "test"

    return event


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--invalid",
        action="store_true",
        help="Send an intentionally invalid event",
    )

    args = parser.parse_args()

    load_dotenv()

    contract_path = os.getenv(
        "CONTRACT_PATH",
        "contracts/order_created.v1.yaml",
    )

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9092",
    )

    contract = load_contract(contract_path)
    event = build_event(invalid=args.invalid)

    if not args.invalid:
        validation_errors = validate_event(event, contract)

        if validation_errors:
            raise ValueError(
                "Producer event is invalid: "
                + "; ".join(validation_errors)
            )

    key_fields = contract.get("key", [])

    if not key_fields:
        raise ValueError("Contract must contain at least one key field")

    message_key = str(event[key_fields[0]])

    producer = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "order-demo-producer",
            "enable.idempotence": True,
        }
    )

    producer.produce(
        topic=contract["topic"],
        key=message_key.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
        callback=delivery_callback,
    )

    remaining_messages = producer.flush(timeout=10)

    if remaining_messages > 0:
        raise RuntimeError(
            f"{remaining_messages} message(s) were not delivered"
        )


if __name__ == "__main__":
    main()