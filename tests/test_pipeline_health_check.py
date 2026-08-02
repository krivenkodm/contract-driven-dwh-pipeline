from pipeline_health_check import PipelineHealth, exit_code


def health(code: int) -> PipelineHealth:
    return PipelineHealth(
        health_code=code,
        overall_health={0: "healthy", 1: "warning", 2: "critical"}[code],
        health_reason="test",
        dead_letter_count_24h=0,
        dq_affected_orders=0,
        orphan_order_count=0,
        latest_completed_status="success",
    )


def test_default_only_fails_on_critical() -> None:
    assert exit_code(health(0), fail_on_warning=False) == 0
    assert exit_code(health(1), fail_on_warning=False) == 0
    assert exit_code(health(2), fail_on_warning=False) == 1


def test_strict_mode_also_fails_on_warning() -> None:
    assert exit_code(health(0), fail_on_warning=True) == 0
    assert exit_code(health(1), fail_on_warning=True) == 1
    assert exit_code(health(2), fail_on_warning=True) == 1
