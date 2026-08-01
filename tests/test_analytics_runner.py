import json

from analytics_runner import (
    CommandOutcome,
    freshness_status,
    overall_status,
    summarize_artifact,
)


def test_summarize_artifact_counts_dbt_node_statuses(tmp_path):
    artifact_path = tmp_path / "run_results.json"
    artifact_path.write_text(
        json.dumps(
            {
                "metadata": {"invocation_id": "invocation-1"},
                "elapsed_time": 1.25,
                "results": [
                    {"status": "success"},
                    {"status": "pass"},
                    {"status": "warn"},
                    {"status": "error"},
                    {"status": "skipped"},
                ],
            }
        ),
        encoding="utf-8",
    )

    summary = summarize_artifact(artifact_path)

    assert summary == {
        "available": True,
        "invocation_id": "invocation-1",
        "elapsed_time": 1.25,
        "total": 5,
        "successful": 2,
        "warned": 1,
        "failed": 2,
        "status_counts": {
            "error": 1,
            "pass": 1,
            "skipped": 1,
            "success": 1,
            "warn": 1,
        },
    }


def test_missing_artifact_has_empty_summary(tmp_path):
    summary = summarize_artifact(tmp_path / "missing.json")

    assert summary["available"] is False
    assert summary["total"] == 0
    assert summary["status_counts"] == {}


def test_freshness_warning_does_not_hide_successful_build():
    freshness = CommandOutcome(
        command=["dbt", "source", "freshness"],
        returncode=0,
        output="",
        artifact={"warned": 1, "failed": 0},
    )
    build = CommandOutcome(
        command=["dbt", "build"],
        returncode=0,
        output="",
        artifact={"warned": 0, "failed": 0},
    )

    freshness_result = freshness_status(freshness)

    assert freshness_result == "warn"
    assert overall_status(freshness_result, build) == "warning"


def test_failed_build_sets_error_status():
    build = CommandOutcome(
        command=["dbt", "build"],
        returncode=1,
        output="failure",
        artifact={"warned": 0, "failed": 1},
    )

    assert overall_status("pass", build) == "error"


def test_missing_build_artifact_sets_error_status():
    build = CommandOutcome(
        command=["dbt", "build"],
        returncode=0,
        output="",
        artifact={
            "available": False,
            "warned": 0,
            "failed": 0,
        },
    )

    assert overall_status("pass", build) == "error"
