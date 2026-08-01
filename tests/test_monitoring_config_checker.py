from copy import deepcopy

from monitoring_config_checker import (
    DASHBOARD_PATH,
    load_json,
    validate_dashboard,
    validate_repository_config,
)


def test_repository_monitoring_configuration_is_valid() -> None:
    assert validate_repository_config() == []


def test_duplicate_dashboard_panel_id_is_rejected() -> None:
    dashboard = deepcopy(load_json(DASHBOARD_PATH))
    dashboard["panels"][1]["id"] = dashboard["panels"][0][
        "id"
    ]

    errors = validate_dashboard(dashboard)

    assert "duplicate panel id: 1" in errors


def test_sensitive_payload_is_not_exposed() -> None:
    dashboard = deepcopy(load_json(DASHBOARD_PATH))
    dashboard["panels"][0]["targets"][0]["rawSql"] = (
        "SELECT raw_payload "
        "FROM dbt_monitoring.monitor_recent_dead_letters"
    )

    errors = validate_dashboard(dashboard)

    assert any("raw_payload" in error for error in errors)
