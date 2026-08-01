# dbt analytics project

This project is the transformation layer for the contract-driven DWH demo.
Validated events remain in `public.raw_*`; dbt is the primary transformation
engine and publishes analytical objects into the `dbt` schema.

The model layers are:

* `staging` — typed, explicitly selected RAW columns;
* `intermediate` — deterministic order lifecycle and orphan-event logic;
* `dds` — incremental current order state and the orphan-event DQ table;
* `snapshots` — history used to detect both old and new affected mart dates;
* `marts` — incremental daily order metrics;
* `monitoring` — live operational views published to `dbt_monitoring` for
  Grafana, including health, run history, volume, DLQ, and DQ metrics.

`dbt build --exclude tag:parity` runs source, model, data and unit tests. Tests
tagged `parity` require the legacy SQL tables in `public` and compare their
business columns with dbt in both directions.

The legacy transformations are retained only as a regression oracle. The
regular `make build-analytics` path does not execute them; use
`make verify-dbt-parity` for an explicit side-by-side comparison.

Use the repository Makefile so connection settings stay consistent:

```bash
make analytics-run
make analytics-history
make dbt-source-freshness
make dbt-build
make verify-dbt-parity
make dbt-docs
make validate-monitoring-config
make monitoring-up
```

`raw_order_created` is the demo freshness heartbeat. The observed runner parses
`sources.json` and `run_results.json`, then stores the combined status in
`public.analytics_run_history`.

The `monitor_pipeline_health` view classifies pipeline state from the latest
observed dbt execution, RAW freshness, DLQ activity, DDS quality flags, and
orphan events. Grafana receives access only to the `dbt_monitoring` schema;
the diagnostic DLQ view intentionally excludes payload columns.
