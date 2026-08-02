#!/usr/bin/env python3
"""Read the dbt monitoring health view and expose an orchestration-friendly exit code."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from typing import Sequence

import psycopg


@dataclass(frozen=True)
class PipelineHealth:
    health_code: int
    overall_health: str
    health_reason: str
    dead_letter_count_24h: int
    dq_affected_orders: int
    orphan_order_count: int
    latest_completed_status: str | None


def read_pipeline_health(postgres_dsn: str) -> PipelineHealth:
    query = """
        SELECT
            health_code,
            overall_health,
            health_reason,
            dead_letter_count_24h,
            dq_affected_orders,
            orphan_order_count,
            latest_completed_status
        FROM dbt_monitoring.monitor_pipeline_health
    """
    with psycopg.connect(postgres_dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()

    if row is None:
        raise RuntimeError("dbt_monitoring.monitor_pipeline_health returned no rows")

    return PipelineHealth(
        health_code=int(row[0]),
        overall_health=str(row[1]),
        health_reason=str(row[2]),
        dead_letter_count_24h=int(row[3]),
        dq_affected_orders=int(row[4]),
        orphan_order_count=int(row[5]),
        latest_completed_status=None if row[6] is None else str(row[6]),
    )


def exit_code(health: PipelineHealth, *, fail_on_warning: bool) -> int:
    threshold = 1 if fail_on_warning else 2
    return 1 if health.health_code >= threshold else 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("POSTGRES_DSN", "postgresql://dwh:dwh@localhost:5432/dwh"),
        help="DWH PostgreSQL connection string",
    )
    parser.add_argument(
        "--fail-on-warning",
        action="store_true",
        help="Return a failure exit code for WARNING as well as CRITICAL",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        health = read_pipeline_health(args.postgres_dsn)
    except Exception as exc:
        print(json.dumps({"overall_health": "unknown", "error": str(exc)}, sort_keys=True))
        return 2

    print(json.dumps(asdict(health), sort_keys=True))
    return exit_code(health, fail_on_warning=args.fail_on_warning)


if __name__ == "__main__":
    raise SystemExit(main())
