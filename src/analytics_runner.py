from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import psycopg
from psycopg.types.json import Jsonb


ANALYTICS_LOCK_ID = 2_026_080_100

SUCCESS_STATUSES = {"pass", "success"}
WARNING_STATUSES = {"warn", "warning"}
ERROR_STATUSES = {
    "error",
    "fail",
    "failed",
    "runtime error",
    "skipped",
}


@dataclass(frozen=True)
class CommandOutcome:
    command: list[str]
    returncode: int
    output: str
    artifact: dict[str, Any]


def summarize_artifact(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "available": False,
            "invocation_id": None,
            "elapsed_time": None,
            "total": 0,
            "successful": 0,
            "warned": 0,
            "failed": 0,
            "status_counts": {},
        }

    artifact = json.loads(path.read_text(encoding="utf-8"))
    status_counts = Counter(
        str(result.get("status", "unknown")).lower()
        for result in artifact.get("results", [])
    )

    return {
        "available": True,
        "invocation_id": artifact.get("metadata", {}).get(
            "invocation_id"
        ),
        "elapsed_time": artifact.get("elapsed_time"),
        "total": sum(status_counts.values()),
        "successful": sum(
            count
            for status, count in status_counts.items()
            if status in SUCCESS_STATUSES
        ),
        "warned": sum(
            count
            for status, count in status_counts.items()
            if status in WARNING_STATUSES
        ),
        "failed": sum(
            count
            for status, count in status_counts.items()
            if status in ERROR_STATUSES
        ),
        "status_counts": dict(sorted(status_counts.items())),
    }


def freshness_status(
    outcome: CommandOutcome | None,
) -> str:
    if outcome is None:
        return "not_run"

    if (
        outcome.returncode != 0
        or not outcome.artifact.get("available", True)
        or outcome.artifact["failed"] > 0
    ):
        return "error"

    if outcome.artifact["warned"] > 0:
        return "warn"

    return "pass"


def overall_status(
    freshness: str,
    build: CommandOutcome,
) -> str:
    if (
        build.returncode != 0
        or not build.artifact.get("available", True)
        or build.artifact["failed"] > 0
    ):
        return "error"

    if freshness in {"warn", "error"}:
        return "warning"

    return "success"


def run_dbt_command(
    command: list[str],
    artifact_path: Path,
) -> CommandOutcome:
    artifact_path.unlink(missing_ok=True)

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        env=os.environ.copy(),
    )
    output = result.stdout + result.stderr

    print(output, end="" if output.endswith("\n") else "\n")

    return CommandOutcome(
        command=command,
        returncode=result.returncode,
        output=output,
        artifact=summarize_artifact(artifact_path),
    )


def dbt_command(
    executable: str,
    action: list[str],
    project_dir: Path,
    profiles_dir: Path,
) -> list[str]:
    return [
        executable,
        *action,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
        "--no-use-colors",
    ]


def notify_webhook(
    webhook_url: str | None,
    payload: dict[str, Any],
) -> None:
    if not webhook_url:
        return

    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except Exception as error:
        print(f"Alert webhook failed: {error}")


def insert_run(
    connection: psycopg.Connection[Any],
    run_id: UUID,
    trigger_type: str,
    started_at: datetime,
) -> None:
    connection.execute(
        """
        INSERT INTO analytics_run_history (
            run_id,
            trigger_type,
            status,
            started_at
        )
        VALUES (%s, %s, 'running', %s)
        """,
        (run_id, trigger_type, started_at),
    )
    connection.commit()


def finish_run(
    connection: psycopg.Connection[Any],
    run_id: UUID,
    status: str,
    started_at: datetime,
    freshness: str,
    build_status: str,
    summary: dict[str, Any],
    error_message: str | None,
) -> None:
    finished_at = datetime.now(UTC)
    build_summary = summary.get("build", {})

    connection.execute(
        """
        UPDATE analytics_run_history
        SET
            status = %s,
            finished_at = %s,
            duration_seconds = %s,
            freshness_status = %s,
            build_status = %s,
            dbt_invocation_id = %s,
            total_nodes = %s,
            successful_nodes = %s,
            warned_nodes = %s,
            failed_nodes = %s,
            result_summary = %s,
            error_message = %s
        WHERE run_id = %s
        """,
        (
            status,
            finished_at,
            (finished_at - started_at).total_seconds(),
            freshness,
            build_status,
            build_summary.get("invocation_id"),
            build_summary.get("total", 0),
            build_summary.get("successful", 0),
            build_summary.get("warned", 0),
            build_summary.get("failed", 0),
            Jsonb(summary),
            error_message,
            run_id,
        ),
    )
    connection.commit()


def output_tail(*outcomes: CommandOutcome | None) -> str | None:
    output = "\n".join(
        outcome.output
        for outcome in outcomes
        if outcome is not None and outcome.output
    )

    return output[-4000:] if output else None


def run_once(args: argparse.Namespace) -> int:
    run_id = uuid4()
    started_at = datetime.now(UTC)
    project_dir = Path(args.project_dir).resolve()
    profiles_dir = Path(args.profiles_dir).resolve()
    target_dir = project_dir / "target"

    with psycopg.connect(args.postgres_dsn) as connection:
        lock_acquired = connection.execute(
            "SELECT pg_try_advisory_lock(%s)",
            (ANALYTICS_LOCK_ID,),
        ).fetchone()

        if not lock_acquired or not lock_acquired[0]:
            print("Another analytics run is active; skipping this cycle")
            return 0

        insert_run(
            connection=connection,
            run_id=run_id,
            trigger_type=args.trigger,
            started_at=started_at,
        )

        freshness_outcome = None
        build_outcome = None

        try:
            if not args.skip_freshness:
                freshness_outcome = run_dbt_command(
                    command=dbt_command(
                        executable=args.dbt_executable,
                        action=["source", "freshness"],
                        project_dir=project_dir,
                        profiles_dir=profiles_dir,
                    ),
                    artifact_path=target_dir / "sources.json",
                )

            freshness_result = freshness_status(freshness_outcome)

            build_outcome = run_dbt_command(
                command=dbt_command(
                    executable=args.dbt_executable,
                    action=[
                        "build",
                        "--exclude",
                        "tag:parity",
                    ],
                    project_dir=project_dir,
                    profiles_dir=profiles_dir,
                ),
                artifact_path=target_dir / "run_results.json",
            )

            status = overall_status(
                freshness=freshness_result,
                build=build_outcome,
            )
            build_result = (
                "success" if status != "error" else "error"
            )
            summary = {
                "freshness": (
                    freshness_outcome.artifact
                    if freshness_outcome is not None
                    else {"available": False}
                ),
                "build": build_outcome.artifact,
            }
            error_message = (
                output_tail(freshness_outcome, build_outcome)
                if status != "success"
                else None
            )

            finish_run(
                connection=connection,
                run_id=run_id,
                status=status,
                started_at=started_at,
                freshness=freshness_result,
                build_status=build_result,
                summary=summary,
                error_message=error_message,
            )

            print(
                json.dumps(
                    {
                        "run_id": str(run_id),
                        "status": status,
                        "freshness_status": freshness_result,
                        "build_status": build_result,
                        "nodes": build_outcome.artifact["total"],
                    },
                    indent=2,
                )
            )

            if status != "success":
                notify_webhook(
                    webhook_url=args.alert_webhook_url,
                    payload={
                        "event": "analytics_run_finished",
                        "run_id": str(run_id),
                        "status": status,
                        "freshness_status": freshness_result,
                        "build_status": build_result,
                    },
                )

            if status == "error":
                return 1

            if status == "warning" and args.fail_on_warning:
                return 2

            return 0

        except Exception as error:
            summary = {
                "freshness": (
                    freshness_outcome.artifact
                    if freshness_outcome is not None
                    else {"available": False}
                ),
                "build": (
                    build_outcome.artifact
                    if build_outcome is not None
                    else {"available": False}
                ),
            }
            finish_run(
                connection=connection,
                run_id=run_id,
                status="error",
                started_at=started_at,
                freshness=freshness_status(freshness_outcome),
                build_status="error",
                summary=summary,
                error_message=str(error),
            )
            notify_webhook(
                webhook_url=args.alert_webhook_url,
                payload={
                    "event": "analytics_run_finished",
                    "run_id": str(run_id),
                    "status": "error",
                    "error": str(error),
                },
            )
            print(f"Analytics run failed: {error}")
            return 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run dbt with freshness checks and audit logging"
    )
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv(
            "POSTGRES_DSN",
            "postgresql://dwh:dwh@localhost:55432/dwh",
        ),
    )
    parser.add_argument(
        "--dbt-executable",
        default=os.getenv("DBT_EXECUTABLE", "dbt"),
    )
    parser.add_argument("--project-dir", default="dbt")
    parser.add_argument("--profiles-dir", default="dbt")
    parser.add_argument(
        "--trigger",
        choices=("manual", "scheduled", "ci"),
        default=os.getenv("ANALYTICS_TRIGGER", "manual"),
    )
    parser.add_argument(
        "--interval-seconds",
        type=int,
        default=int(os.getenv("ANALYTICS_RUN_INTERVAL_SECONDS", "0")),
        help="Repeat forever with this interval; zero runs once",
    )
    parser.add_argument("--skip-freshness", action="store_true")
    parser.add_argument("--fail-on-warning", action="store_true")
    parser.add_argument(
        "--alert-webhook-url",
        default=os.getenv("ALERT_WEBHOOK_URL"),
    )

    args = parser.parse_args()

    if args.interval_seconds < 0:
        parser.error("--interval-seconds must be zero or positive")

    return args


def main() -> int:
    args = parse_args()

    if args.interval_seconds == 0:
        return run_once(args)

    while True:
        run_once(args)
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
