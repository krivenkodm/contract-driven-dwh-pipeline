import json
from pathlib import Path
from typing import Any

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONITORING_ROOT = PROJECT_ROOT / "monitoring" / "grafana"
DASHBOARD_PATH = (
    MONITORING_ROOT
    / "dashboards"
    / "contract-dwh-operations.json"
)
DATASOURCE_PATH = (
    MONITORING_ROOT
    / "provisioning"
    / "datasources"
    / "postgres.yml"
)
DASHBOARD_PROVIDER_PATH = (
    MONITORING_ROOT
    / "provisioning"
    / "dashboards"
    / "dashboards.yml"
)
ALERT_RULE_PATH = (
    MONITORING_ROOT
    / "provisioning"
    / "alerting"
    / "pipeline-health.yml"
)

DATASOURCE_UID = "contract-dwh-postgres"
DASHBOARD_UID = "contract-dwh-operations"
MONITORING_SCHEMA = "dbt_monitoring."
SENSITIVE_PAYLOAD_COLUMNS = {
    "event_payload",
    "original_payload",
    "raw_payload",
}


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_yaml(path: Path) -> dict[str, Any]:
    document = yaml.safe_load(path.read_text(encoding="utf-8"))

    if not isinstance(document, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")

    return document


def validate_dashboard(
    dashboard: dict[str, Any],
) -> list[str]:
    errors: list[str] = []

    if dashboard.get("uid") != DASHBOARD_UID:
        errors.append(
            f"dashboard uid must be {DASHBOARD_UID!r}"
        )

    panels = dashboard.get("panels")

    if not isinstance(panels, list) or not panels:
        return [*errors, "dashboard must contain panels"]

    panel_ids: set[int] = set()
    panel_titles: set[str] = set()

    for panel in panels:
        panel_id = panel.get("id")
        title = panel.get("title")

        if not isinstance(panel_id, int):
            errors.append("every panel must have an integer id")
        elif panel_id in panel_ids:
            errors.append(f"duplicate panel id: {panel_id}")
        else:
            panel_ids.add(panel_id)

        if not isinstance(title, str) or not title.strip():
            errors.append(
                f"panel {panel_id!r} must have a title"
            )
        elif title in panel_titles:
            errors.append(f"duplicate panel title: {title}")
        else:
            panel_titles.add(title)

        targets = panel.get("targets")

        if not isinstance(targets, list) or not targets:
            errors.append(f"panel {title!r} must have targets")
            continue

        for target in targets:
            datasource = target.get("datasource", {})
            raw_sql = target.get("rawSql")

            if datasource.get("uid") != DATASOURCE_UID:
                errors.append(
                    f"panel {title!r} must use {DATASOURCE_UID}"
                )

            if not isinstance(raw_sql, str) or not raw_sql.strip():
                errors.append(
                    f"panel {title!r} must use an explicit SQL query"
                )
                continue

            normalized_sql = raw_sql.lower()

            if MONITORING_SCHEMA not in normalized_sql:
                errors.append(
                    f"panel {title!r} must query {MONITORING_SCHEMA}"
                )

            exposed_columns = sorted(
                column
                for column in SENSITIVE_PAYLOAD_COLUMNS
                if column in normalized_sql
            )

            if exposed_columns:
                errors.append(
                    f"panel {title!r} exposes payload columns: "
                    f"{', '.join(exposed_columns)}"
                )

    return errors


def validate_provisioning(
    datasource: dict[str, Any],
    dashboard_provider: dict[str, Any],
    alert_rules: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    datasources = datasource.get("datasources", [])

    if not any(
        item.get("uid") == DATASOURCE_UID
        and item.get("type") == "postgres"
        for item in datasources
    ):
        errors.append(
            "PostgreSQL datasource provisioning is missing"
        )

    providers = dashboard_provider.get("providers", [])

    if not any(
        provider.get("options", {}).get("path")
        == "/var/lib/grafana/dashboards"
        for provider in providers
    ):
        errors.append("dashboard filesystem provider is missing")

    groups = alert_rules.get("groups", [])
    rules = [
        rule
        for group in groups
        for rule in group.get("rules", [])
    ]

    if not any(
        rule.get("uid") == "contract_dwh_pipeline_critical"
        and rule.get("dashboardUid") == DASHBOARD_UID
        for rule in rules
    ):
        errors.append("critical pipeline alert rule is missing")

    return errors


def validate_repository_config() -> list[str]:
    dashboard = load_json(DASHBOARD_PATH)
    datasource = load_yaml(DATASOURCE_PATH)
    dashboard_provider = load_yaml(DASHBOARD_PROVIDER_PATH)
    alert_rules = load_yaml(ALERT_RULE_PATH)

    return [
        *validate_dashboard(dashboard),
        *validate_provisioning(
            datasource=datasource,
            dashboard_provider=dashboard_provider,
            alert_rules=alert_rules,
        ),
    ]


def main() -> int:
    errors = validate_repository_config()

    if errors:
        print("Monitoring configuration check failed:")

        for error in errors:
            print(f"  - {error}")

        return 1

    dashboard = load_json(DASHBOARD_PATH)
    print(
        "Monitoring configuration check passed "
        f"({len(dashboard['panels'])} dashboard panels)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
