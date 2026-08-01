import os
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import make_conninfo

from tests.integration.db_support import (
    DatabaseFactory,
    IntegrationDatabase,
    run_database_make_target,
)

@pytest.fixture(scope="session")
def database_factory() -> Iterator[DatabaseFactory]:
    if os.getenv("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip(
            "Set RUN_INTEGRATION_TESTS=1 to run integration tests"
        )

    admin_dsn = os.environ.get(
        "INTEGRATION_POSTGRES_ADMIN_DSN"
    )

    if not admin_dsn:
        pytest.fail(
            "INTEGRATION_POSTGRES_ADMIN_DSN is required"
        )

    databases: list[IntegrationDatabase] = []

    def create_database() -> IntegrationDatabase:
        database_name = (
            "contract_dwh_it_"
            f"{uuid4().hex[:16]}"
        )

        with psycopg.connect(
            admin_dsn,
            autocommit=True,
        ) as connection:
            connection.execute(
                sql.SQL("CREATE DATABASE {}").format(
                    sql.Identifier(database_name)
                )
            )

        database = IntegrationDatabase(
            name=database_name,
            dsn=make_conninfo(
                admin_dsn,
                dbname=database_name,
            ),
        )
        databases.append(database)

        return database

    yield create_database

    with psycopg.connect(
        admin_dsn,
        autocommit=True,
    ) as connection:
        for database in databases:
            if not database.name.startswith(
                "contract_dwh_it_"
            ):
                raise RuntimeError(
                    "Refusing to drop unexpected database: "
                    f"{database.name}"
                )

            connection.execute(
                sql.SQL(
                    "DROP DATABASE IF EXISTS {} WITH (FORCE)"
                ).format(
                    sql.Identifier(database.name)
                )
            )


@pytest.fixture(scope="module")
def initialized_database(
    database_factory: DatabaseFactory,
) -> IntegrationDatabase:
    database = database_factory()
    run_database_make_target(
        database=database,
        target="init-db",
    )

    return database
