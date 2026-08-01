import os
import time
from pathlib import Path

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from contract_registry import Contract, load_contracts


DEFAULT_BROKER_WAIT_SECONDS = 60
BROKER_RETRY_DELAY_SECONDS = 1.0


def wait_for_broker(
    admin_client: AdminClient,
    timeout_seconds: int = DEFAULT_BROKER_WAIT_SECONDS,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            metadata = admin_client.list_topics(timeout=3)

            if metadata.brokers:
                print("Kafka broker is available", flush=True)
                return

        except Exception as error:
            last_error = error

            print(
                f"Kafka broker is not ready: {error}",
                flush=True,
            )

        time.sleep(BROKER_RETRY_DELAY_SECONDS)

    raise RuntimeError(
        "Kafka broker did not become available within "
        f"{timeout_seconds} seconds. Last error: {last_error}"
    )


def load_all_contracts(
    contracts_dir: Path,
) -> list[Contract]:
    contracts = load_contracts(
        contracts_dir
    )

    for contract in contracts:
        print(
            "Loaded contract: "
            f"{contract['name']} "
            f"v{contract['version']}",
            flush=True,
        )

    return contracts


def get_contract_topics(
    contracts_dir: Path,
) -> list[str]:
    contracts = load_all_contracts(contracts_dir)

    topics: set[str] = set()

    for contract in contracts:
        topic = contract.get("topic")

        if not topic:
            raise RuntimeError(
                "Contract does not contain a non-empty topic: "
                f"{contract.get('name', '<unknown>')}"
            )

        topics.add(str(topic))

    return sorted(topics)


def create_missing_topics(
    admin_client: AdminClient,
    topics: list[str],
) -> None:
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics)

    missing_topics = [
        topic
        for topic in topics
        if topic not in existing_topics
    ]

    for topic in topics:
        if topic in existing_topics:
            print(
                f"Topic already exists: {topic}",
                flush=True,
            )

    if not missing_topics:
        print(
            "All contract topics already exist",
            flush=True,
        )
        return

    definitions = [
        NewTopic(
            topic=topic,
            num_partitions=1,
            replication_factor=1,
        )
        for topic in missing_topics
    ]

    futures = admin_client.create_topics(definitions)

    for topic, future in futures.items():
        try:
            future.result()

            print(
                f"Topic created: {topic}",
                flush=True,
            )

        except KafkaException as error:
            kafka_error = error.args[0] if error.args else None

            if (
                isinstance(kafka_error, KafkaError)
                and kafka_error.code()
                == KafkaError.TOPIC_ALREADY_EXISTS
            ):
                print(
                    f"Topic already exists: {topic}",
                    flush=True,
                )
                continue

            raise RuntimeError(
                f"Failed to create topic {topic}: {error}"
            ) from error


def main() -> None:
    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:19092",
    )

    contracts_dir = Path(
        os.getenv(
            "CONTRACTS_DIR",
            "contracts",
        )
    )

    print(
        f"Kafka bootstrap servers: {bootstrap_servers}",
        flush=True,
    )
    print(
        f"Contracts directory: {contracts_dir}",
        flush=True,
    )

    topics = get_contract_topics(contracts_dir)

    print(
        "Topics found in contracts:",
        flush=True,
    )

    for topic in topics:
        print(
            f"  - {topic}",
            flush=True,
        )

    admin_client = AdminClient(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "contract-topic-manager",
        }
    )

    wait_for_broker(admin_client)
    create_missing_topics(admin_client, topics)

    print(
        "Topic initialization completed successfully",
        flush=True,
    )


if __name__ == "__main__":
    main()
