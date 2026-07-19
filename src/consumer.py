import argparse
import json
import os
import time
from json import JSONDecodeError
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from dotenv import load_dotenv

from contract_registry import (
    load_contracts,
    map_contracts_by_topic,
)
from dwh_writer import DwhWriter
from validator import validate_event


def parse_message(
    message: Message,
) -> tuple[dict[str, Any] | None, list[str]]:
    try:
        raw_value = message.value().decode("utf-8")
        payload = json.loads(raw_value)

    except UnicodeDecodeError as error:
        return None, [f"Message is not valid UTF-8: {error}"]

    except JSONDecodeError as error:
        return None, [f"Message is not valid JSON: {error}"]

    if not isinstance(payload, dict):
        return None, ["Message payload must be a JSON object"]

    return payload, []


def process_message(
    message: Message,
    contract: dict[str, Any],
    writer: DwhWriter,
) -> None:
    payload, parsing_errors = parse_message(message)

    errors = list(parsing_errors)

    if payload is not None:
        errors.extend(
            validate_event(
                event=payload,
                contract=contract,
            )
        )

    metadata = {
        "kafka_topic": message.topic(),
        "kafka_partition": message.partition(),
        "kafka_offset": message.offset(),
    }

    if errors:
        inserted = writer.write_dead_letter(
            payload=payload,
            error_message="; ".join(errors),
            **metadata,
        )

        status = "saved" if inserted else "already exists"

        print(
            f"Invalid event {status} in dead letter: "
            f"{metadata}; errors={errors}"
        )
        return

    if payload is None:
        raise RuntimeError(
            "Payload cannot be None after successful validation"
        )

    inserted = writer.write_valid_event(
        contract=contract,
        event=payload,
        **metadata,
    )

    status = "inserted" if inserted else "already exists"

    print(
        f"Valid event {status} in raw table: "
        f"{metadata}"
    )


def main() -> None:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--once",
        action="store_true",
        help="Process one message and exit",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=15,
        help="Timeout in seconds for --once mode",
    )

    args = parser.parse_args()

    load_dotenv()

    contracts = load_contracts(
        os.getenv(
            "CONTRACTS_DIR",
            "contracts",
        )
    )

    contracts_by_topic = map_contracts_by_topic(
        contracts
    )

    topics = sorted(contracts_by_topic)

    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092",
            ),
            "group.id": os.getenv(
                "CONSUMER_GROUP_ID",
                "contract-dwh-loader-v1",
            ),
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    writer = DwhWriter(
        os.getenv(
            "POSTGRES_DSN",
            "postgresql://dwh:dwh@localhost:5432/dwh",
        )
    )

    consumer.subscribe(topics)

    started_at = time.monotonic()

    print(
        "Consumer started. Topics: "
        f"{', '.join(topics)}. "
        "Press Ctrl+C to stop."
    )

    try:
        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                if (
                    args.once
                    and time.monotonic() - started_at >= args.timeout
                ):
                    print("No new messages received")
                    break

                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise RuntimeError(message.error())
            
            contract = contracts_by_topic.get(
                message.topic()
            )

            if contract is None:
                raise RuntimeError(
                    "No contract registered for topic: "
                    f"{message.topic()}"
                )

            process_message(
                message=message,
                contract=contract,
                writer=writer,
            )

            consumer.commit(
                message=message,
                asynchronous=False,
            )

            if args.once:
                break

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        consumer.close()
        writer.close()


if __name__ == "__main__":
    main()