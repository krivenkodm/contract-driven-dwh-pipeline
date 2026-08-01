# dbt analytics project

This project is the transformation layer for the contract-driven DWH demo.
Validated events remain in `public.raw_*`; dbt objects are isolated in the
`dbt` schema during the migration from the original SQL implementation.

The model layers are:

* `staging` — typed, explicitly selected RAW columns;
* `intermediate` — deterministic order lifecycle and orphan-event logic;
* `dds` — incremental current order state and the orphan-event DQ table;
* `snapshots` — history used to detect both old and new affected mart dates;
* `marts` — incremental daily order metrics.

`dbt build --exclude tag:parity` runs source, model, data and unit tests. Tests
tagged `parity` require the legacy SQL tables in `public` and compare their
business columns with dbt in both directions.

Use the repository Makefile so connection settings stay consistent:

```bash
make dbt-build
make dbt-parity
make dbt-docs
```
