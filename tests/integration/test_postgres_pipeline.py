import json
from datetime import date
from pathlib import Path

import psycopg
import pytest

from contract_registry import get_contract, load_contracts
from ddl_generator import generate_raw_ddl
from dwh_writer import DwhWriter

from tests.integration.db_support import (
    DatabaseFactory,
    IntegrationDatabase,
    run_dbt,
    run_database_make_target,
)


pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONTRACTS_DIR = PROJECT_ROOT / "contracts"
RAW_SQL_DIR = PROJECT_ROOT / "sql" / "raw"

PROVENANCE_COLUMNS = {
    "contract_name",
    "contract_version",
    "original_payload",
    "raw_payload",
}

PROVENANCE_COLUMN_TYPES = {
    "contract_name": "character varying",
    "contract_version": "integer",
    "original_payload": "jsonb",
    "raw_payload": "bytea",
}


def test_fresh_database_schema_and_migrations_are_idempotent(
    initialized_database: IntegrationDatabase,
) -> None:
    with psycopg.connect(
        initialized_database.dsn
    ) as connection:
        migration_versions = connection.execute(
            """
            SELECT version
            FROM schema_migrations
            ORDER BY version
            """
        ).fetchall()

        assert migration_versions == [
            (1,),
            (2,),
            (3,),
            (4,),
            (5,),
            (6,),
            (7,),
        ]

        columns = connection.execute(
            """
            SELECT
                table_name,
                column_name,
                is_nullable,
                data_type
            FROM information_schema.columns
            WHERE table_name IN (
                'raw_order_created',
                'raw_order_paid',
                'raw_order_cancelled',
                'dead_letter_events'
            )
              AND column_name IN (
                'contract_name',
                'contract_version',
                'original_payload',
                'raw_payload'
            )
            """
        ).fetchall()

    assert len(columns) == 12

    for table_name, column_name, is_nullable, data_type in columns:
        assert column_name in PROVENANCE_COLUMNS
        assert data_type == PROVENANCE_COLUMN_TYPES[column_name]

        if column_name == "raw_payload":
            assert table_name == "dead_letter_events"
            assert is_nullable == "YES"
        else:
            assert is_nullable == "NO"

    migration_output = run_database_make_target(
        database=initialized_database,
        target="migrate",
    )

    assert "SKIP  007_add_contract_provenance_to_raw.sql" in (
        migration_output
    )


def test_writer_persists_defaults_provenance_and_dlq_bytes(
    initialized_database: IntegrationDatabase,
) -> None:
    contracts = load_contracts(CONTRACTS_DIR)
    order_contract = get_contract(
        contracts=contracts,
        name="order_created",
        version=2,
    )
    order_event = {
        "order_id": "ord_integration_writer",
        "customer_id": "customer_integration",
        "amount": 1500.50,
        "currency": "RUB",
        "created_at": "2026-08-01T12:00:00Z",
    }

    default_contract = {
        "name": "default_event",
        "version": 1,
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

    with psycopg.connect(
        initialized_database.dsn
    ) as connection:
        connection.execute(
            generate_raw_ddl(default_contract)
        )
        connection.commit()

    writer = DwhWriter(initialized_database.dsn)

    try:
        assert writer.write_valid_event(
            contract=order_contract,
            event=order_event,
            kafka_topic=order_contract["topic"],
            kafka_partition=0,
            kafka_offset=100,
        ) is True

        assert writer.write_valid_event(
            contract=order_contract,
            event=order_event,
            kafka_topic=order_contract["topic"],
            kafka_partition=0,
            kafka_offset=100,
        ) is False

        assert writer.write_valid_event(
            contract=default_contract,
            event={"event_id": "event_1"},
            kafka_topic="test.default_event.v1",
            kafka_partition=0,
            kafka_offset=101,
        ) is True

        invalid_bytes = b"\xffnot-json"

        assert writer.write_dead_letter(
            contract=order_contract,
            raw_payload=invalid_bytes,
            payload=None,
            error_message="invalid UTF-8",
            kafka_topic=order_contract["topic"],
            kafka_partition=0,
            kafka_offset=102,
        ) is True

        assert writer.write_dead_letter(
            contract=order_contract,
            raw_payload=invalid_bytes,
            payload=None,
            error_message="invalid UTF-8",
            kafka_topic=order_contract["topic"],
            kafka_partition=0,
            kafka_offset=102,
        ) is False

        assert writer.write_dead_letter(
            contract=order_contract,
            raw_payload=None,
            payload=None,
            error_message="tombstone",
            kafka_topic=order_contract["topic"],
            kafka_partition=0,
            kafka_offset=103,
        ) is True

    finally:
        writer.close()

    with psycopg.connect(
        initialized_database.dsn
    ) as connection:
        raw_row = connection.execute(
            """
            SELECT
                contract_name,
                contract_version,
                original_payload
            FROM raw_order_created
            WHERE order_id = %s
            """,
            (order_event["order_id"],),
        ).fetchone()

        default_row = connection.execute(
            """
            SELECT source, original_payload
            FROM raw_default_event
            WHERE event_id = 'event_1'
            """
        ).fetchone()

        dlq_rows = connection.execute(
            """
            SELECT kafka_offset, raw_payload
            FROM dead_letter_events
            WHERE kafka_offset IN (102, 103)
            ORDER BY kafka_offset
            """
        ).fetchall()

    assert raw_row == (
        "order_created",
        2,
        order_event,
    )
    assert default_row == (
        "unknown",
        {"event_id": "event_1"},
    )
    assert dlq_rows == [
        (102, invalid_bytes),
        (103, None),
    ]


def test_populated_legacy_schema_is_backfilled(
    database_factory: DatabaseFactory,
) -> None:
    database = database_factory()

    removed_columns = {
        "contract_name",
        "contract_version",
        "original_payload",
        "raw_payload",
    }

    with psycopg.connect(database.dsn) as connection:
        for sql_path in sorted(RAW_SQL_DIR.glob("*.sql")):
            sql_text = sql_path.read_text(encoding="utf-8")
            legacy_sql = "\n".join(
                line
                for line in sql_text.splitlines()
                if not any(
                    column_name in line
                    for column_name in removed_columns
                )
            )
            connection.execute(legacy_sql)

        connection.execute(
            """
            INSERT INTO raw_order_created (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_load_dttm,
                order_id,
                customer_id,
                amount,
                currency,
                created_at,
                source_channel
            )
            VALUES (
                'ecommerce.order_created.v2',
                0,
                200,
                '2026-08-01T12:00:01Z',
                'ord_legacy',
                'customer_legacy',
                250.50,
                'RUB',
                '2026-08-01T12:00:00Z',
                'legacy'
            )
            """
        )

        legacy_payload = {
            "order_id": "ord_invalid_legacy",
            "amount": "invalid",
        }

        connection.execute(
            """
            INSERT INTO dead_letter_events (
                kafka_topic,
                kafka_partition,
                kafka_offset,
                event_payload,
                error_message
            )
            VALUES (
                'ecommerce.order_created.v2',
                0,
                201,
                %s::jsonb,
                'legacy validation error'
            )
            """,
            (json.dumps(legacy_payload),),
        )
        connection.commit()

    run_database_make_target(
        database=database,
        target="migrate",
    )

    with psycopg.connect(database.dsn) as connection:
        raw_row = connection.execute(
            """
            SELECT
                contract_name,
                contract_version,
                original_payload
            FROM raw_order_created
            WHERE order_id = 'ord_legacy'
            """
        ).fetchone()

        dlq_row = connection.execute(
            """
            SELECT
                contract_name,
                contract_version,
                raw_payload
            FROM dead_letter_events
            WHERE kafka_offset = 201
            """
        ).fetchone()

    assert raw_row is not None
    assert raw_row[0:2] == (
        "order_created",
        2,
    )
    assert raw_row[2]["order_id"] == "ord_legacy"

    assert dlq_row is not None
    assert dlq_row[0:2] == (
        "order_created",
        2,
    )
    assert json.loads(dlq_row[2]) == legacy_payload


def test_dbt_incremental_models_match_legacy_and_move_mart_date(
    database_factory: DatabaseFactory,
) -> None:
    database = database_factory()
    run_database_make_target(
        database=database,
        target="init-db",
    )

    contracts = load_contracts(CONTRACTS_DIR)
    created_contract = get_contract(
        contracts=contracts,
        name="order_created",
        version=2,
    )
    paid_contract = get_contract(
        contracts=contracts,
        name="order_paid",
        version=1,
    )
    cancelled_contract = get_contract(
        contracts=contracts,
        name="order_cancelled",
        version=1,
    )

    order_id = "ord_dbt_incremental"
    writer = DwhWriter(database.dsn)

    try:
        assert writer.write_valid_event(
            contract=created_contract,
            event={
                "order_id": order_id,
                "customer_id": "customer_original",
                "amount": 1200.00,
                "currency": "RUB",
                "created_at": "2026-08-02T10:00:00Z",
                "source_channel": "web",
            },
            kafka_topic=created_contract["topic"],
            kafka_partition=0,
            kafka_offset=300,
        ) is True

        assert writer.write_valid_event(
            contract=paid_contract,
            event={
                "order_id": order_id,
                "payment_id": "payment_dbt_incremental",
                "paid_amount": 1200.00,
                "currency": "RUB",
                "paid_at": "2026-08-02T11:00:00Z",
            },
            kafka_topic=paid_contract["topic"],
            kafka_partition=0,
            kafka_offset=301,
        ) is True

        run_database_make_target(
            database=database,
            target="build-legacy-analytics",
        )
        run_dbt(
            database,
            "build",
            "--exclude",
            "tag:parity",
        )
        run_dbt(
            database,
            "test",
            "--select",
            "tag:parity",
        )

        with psycopg.connect(database.dsn) as connection:
            first_dds_row = connection.execute(
                """
                SELECT
                    status,
                    created_at::date,
                    processed_dttm
                FROM dbt.dds_orders
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()

            first_mart_rows = connection.execute(
                """
                SELECT order_dt, paid_orders_cnt
                FROM dbt.mart_daily_orders
                ORDER BY order_dt
                """
            ).fetchall()

        assert first_dds_row is not None
        assert first_dds_row[0:2] == (
            "paid",
            date(2026, 8, 2),
        )
        assert first_mart_rows == [
            (date(2026, 8, 2), 1),
        ]

        run_dbt(
            database,
            "build",
            "--exclude",
            "tag:parity",
        )

        with psycopg.connect(database.dsn) as connection:
            unchanged_processed_dttm = connection.execute(
                """
                SELECT processed_dttm
                FROM dbt.dds_orders
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()

            unchanged_history_rows = connection.execute(
                """
                SELECT count(*)
                FROM dbt.dds_orders_history
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()

        assert unchanged_processed_dttm == (first_dds_row[2],)
        assert unchanged_history_rows == (1,)

        assert writer.write_valid_event(
            contract=created_contract,
            event={
                "order_id": order_id,
                "customer_id": "customer_late_arrival",
                "amount": 1100.00,
                "currency": "RUB",
                "created_at": "2026-08-01T12:00:00Z",
                "source_channel": "mobile_app",
            },
            kafka_topic=created_contract["topic"],
            kafka_partition=0,
            kafka_offset=302,
        ) is True

        assert writer.write_valid_event(
            contract=cancelled_contract,
            event={
                "order_id": order_id,
                "cancellation_id": "cancel_dbt_incremental",
                "cancellation_reason": "customer_request",
                "cancelled_at": "2026-08-01T12:30:00Z",
            },
            kafka_topic=cancelled_contract["topic"],
            kafka_partition=0,
            kafka_offset=303,
        ) is True

        run_database_make_target(
            database=database,
            target="build-legacy-analytics",
        )
        run_dbt(
            database,
            "build",
            "--exclude",
            "tag:parity",
        )
        run_dbt(
            database,
            "test",
            "--select",
            "tag:parity",
        )

        with psycopg.connect(database.dsn) as connection:
            moved_dds_row = connection.execute(
                """
                SELECT
                    customer_id,
                    order_amount,
                    created_at::date,
                    status,
                    duplicate_created_events_cnt,
                    dq_payment_after_cancellation_flg
                FROM dbt.dds_orders
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()

            moved_mart_rows = connection.execute(
                """
                SELECT
                    order_dt,
                    created_orders_cnt,
                    cancelled_orders_cnt,
                    orders_with_dq_cnt
                FROM dbt.mart_daily_orders
                ORDER BY order_dt
                """
            ).fetchall()

            history_state = connection.execute(
                """
                SELECT
                    count(*),
                    count(*) FILTER (WHERE dbt_valid_to IS NULL)
                FROM dbt.dds_orders_history
                WHERE order_id = %s
                """,
                (order_id,),
            ).fetchone()

        assert moved_dds_row is not None
        assert moved_dds_row[0] == "customer_late_arrival"
        assert float(moved_dds_row[1]) == 1100.00
        assert moved_dds_row[2:] == (
            date(2026, 8, 1),
            "cancelled",
            1,
            True,
        )
        assert moved_mart_rows == [
            (date(2026, 8, 1), 1, 1, 1),
        ]
        assert history_state == (2, 1)

    finally:
        writer.close()
