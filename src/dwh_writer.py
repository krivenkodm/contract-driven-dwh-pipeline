from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb


class DwhWriter:
    def __init__(self, dsn: str) -> None:
        self.connection = psycopg.connect(dsn)

    def close(self) -> None:
        self.connection.close()

    def write_valid_event(
        self,
        contract: dict[str, Any],
        event: dict[str, Any],
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> bool:
        table_name = f"raw_{contract['name']}"

        event_fields = [
            field["name"]
            for field in contract["schema"]["fields"]
        ]

        columns = [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_load_dttm",
            "contract_name",
            "contract_version",
            "original_payload",
            *event_fields,
        ]

        values = [
            kafka_topic,
            kafka_partition,
            kafka_offset,
            sql.SQL("CURRENT_TIMESTAMP"),
            contract["name"],
            contract["version"],
            Jsonb(event),
            *[
                (
                    event[field["name"]]
                    if field["name"] in event
                    else field.get("default")
                )
                for field in contract["schema"]["fields"]
            ],
        ]

        value_parts = []

        query_parameters = []

        for value in values:
            if isinstance(value, sql.Composable):
                value_parts.append(value)
            else:
                value_parts.append(sql.Placeholder())
                query_parameters.append(value)

        query = sql.SQL(
            """
            INSERT INTO {table_name} ({columns})
            VALUES ({values})
            ON CONFLICT (
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            DO NOTHING
            """
        ).format(
            table_name=sql.Identifier(table_name),
            columns=sql.SQL(", ").join(
                sql.Identifier(column)
                for column in columns
            ),
            values=sql.SQL(", ").join(value_parts),
        )

        try:
            cursor = self.connection.execute(
                query,
                query_parameters,
            )
            self.connection.commit()

            return cursor.rowcount > 0

        except Exception:
            self.connection.rollback()
            raise

    def write_dead_letter(
        self,
        contract: dict[str, Any],
        raw_payload: bytes | None,
        payload: dict[str, Any] | None,
        error_message: str,
        kafka_topic: str,
        kafka_partition: int,
        kafka_offset: int,
    ) -> bool:
        query = """
            INSERT INTO dead_letter_events (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                contract_name,
                contract_version,
                raw_payload,
                event_payload,
                error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                kafka_topic,
                kafka_partition,
                kafka_offset
            )
            DO NOTHING
        """

        try:
            cursor = self.connection.execute(
                query,
                (
                    kafka_topic,
                    kafka_partition,
                    kafka_offset,
                    contract["name"],
                    contract["version"],
                    raw_payload,
                    Jsonb(payload) if payload is not None else None,
                    error_message,
                ),
            )
            self.connection.commit()

            return cursor.rowcount > 0

        except Exception:
            self.connection.rollback()
            raise
