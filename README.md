# Contract-Driven DWH Pipeline

Demo project showing how **data contracts** can drive a DWH ingestion pipeline from Kafka-compatible streaming to analytical marts.

The project uses **Redpanda** as a lightweight Kafka-compatible broker for local development.

```text
Data Contract → Kafka-compatible Topic → Validation → RAW → dbt → DDS → Mart
```

---

## 1. Goal

The goal of this project is to demonstrate how backend events can be safely loaded into a DWH using data contracts.

A data contract defines:

* event schema
* field types
* required fields
* business keys
* enum values
* data quality rules
* schema compatibility rules
* data owner

The pipeline uses this contract to:

* generate raw DWH table DDL
* validate incoming events
* load valid events into the raw layer
* save invalid events into a dead-letter table
* preserve the validated JSON payload and contract version in RAW
* preserve the original message bytes and contract version in DLQ
* build DDS and mart tables
* surface basic duplicate-event data quality flags

---

## 2. Architecture

```text
┌─────────────────┐
│ Data Contract   │
│ YAML            │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ Producer        │
└────────┬────────┘
         │
         v
┌────────────────────────┐
│ Redpanda               │
│ Kafka-compatible topic │
└────────┬───────────────┘
         │
         v
┌─────────────────────┐
│ Contract Validator  │
└────────┬────────────┘
         │
         ├── invalid events ──> Dead Letter Table
         │
         v
┌─────────────────────┐
│ Raw DWH Layer       │
└──────────┬──────────┘
           v
┌─────────────────────┐
│ dbt staging +       │
│ intermediate models │
└──────────┬──────────┘
           v
┌─────────────────────┐
│ Incremental DDS +   │
│ order history       │
└──────────┬──────────┘
           v
┌─────────────────────┐
│ Incremental Mart    │
└─────────────────────┘
```

---

## 3. Tech Stack

* Python
* Redpanda as a Kafka-compatible broker
* PostgreSQL as a demo DWH
* YAML data contracts
* Docker Compose
* pytest
* dbt Core with the PostgreSQL adapter
* GitHub Actions
* Makefile

---

## 4. Repository Structure

```text
contract-driven-dwh-pipeline/
│
├── contracts/
│   ├── data_contract.schema.json
│   ├── order_created.v1.yaml
│   ├── order_created.v2.yaml
│   ├── order_paid.v1.yaml
│   └── order_cancelled.v1.yaml
│
├── src/
│   ├── contract_registry.py
│   ├── compatibility_checker.py
│   ├── ddl_generator.py
│   ├── validator.py
│   ├── producer.py
│   ├── consumer.py
│   ├── dwh_writer.py
│   ├── analytics_runner.py
│   └── topic_manager.py
│
├── sql/
│   ├── raw/
│   ├── dds/
│   ├── mart/
│   ├── migrations/
│   └── tests/
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── dds/
│   │   └── marts/
│   ├── snapshots/
│   └── tests/
│
├── scripts/
│   ├── migrate.sh
│   └── e2e_smoke.sh
│
├── tests/
│   ├── integration/
│   └── test_*.py
│
├── .github/workflows/
│   └── ci.yml
│
├── docker-compose.yml
├── Dockerfile.analytics
├── Makefile
├── requirements.txt
├── requirements-dbt.txt
└── README.md
```

---

## 5. Example Data Contract

```yaml
name: order_created
version: 1
topic: ecommerce.order_created.v1
owner: orders-team

key:
  - order_id

schema:
  allow_extra_fields: false
  fields:
    - name: order_id
      type: string
      nullable: false

    - name: customer_id
      type: string
      nullable: false

    - name: amount
      type: decimal(12,2)
      nullable: false

    - name: currency
      type: string
      nullable: false
      enum: ["RUB", "USD", "EUR"]

    - name: created_at
      type: timestamp
      nullable: false

quality:
  unique_key:
    - order_id

  not_null:
    - order_id
    - customer_id
    - amount
    - created_at

compatibility:
  mode: backward
```

---

## 6. Example Event

```json
{
  "order_id": "ord_1001",
  "customer_id": "cust_777",
  "amount": 1500.50,
  "currency": "RUB",
  "created_at": "2026-07-15T12:00:00Z"
}
```

---

## 7. Generated Raw Table

The raw table is generated from the contract.

```sql
CREATE TABLE IF NOT EXISTS raw_order_created (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_load_dttm timestamptz NOT NULL,
    contract_name varchar NOT NULL,
    contract_version integer NOT NULL,
    original_payload jsonb NOT NULL,

    order_id varchar NOT NULL,
    customer_id varchar NOT NULL,
    amount numeric(12,2) NOT NULL,
    currency varchar NOT NULL,
    created_at timestamptz NOT NULL,
    source_channel varchar,

    CONSTRAINT uq_raw_order_created_kafka_message
        UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
```

The raw layer stores typed event fields, Kafka coordinates, the contract name
and version used for validation, and the complete validated JSON object. This
makes replay and schema-version debugging independent of the current contract.

Before a contract is used, `contract_registry.py` validates it against
`contracts/data_contract.schema.json` and then performs semantic checks that
JSON Schema alone cannot express. These include field-reference validity,
non-null business keys, decimal precision and scale, default values, contiguous
versions, `quality.not_null` consistency, and agreement between the filename,
topic and version.

---

## 8. DWH Layers

### Raw

Stores source events almost as they arrived.

Purpose:

* store validated event fields in typed columns
* store Kafka metadata
* support replay and debugging

Example:

```text
raw_order_created
```

### DDS

Stores cleaned business entities.

Example:

```text
dds_orders
```

The dbt implementation builds this entity incrementally in the `dbt` schema.
New RAW identity values determine the affected orders, while the complete event
history for those orders is recalculated. This preserves late-arriving event
handling without rebuilding every order.

Possible fields:

```text
order_id
customer_id
created_at
paid_at
cancelled_at
status
amount
currency
```

### Mart

Stores analytics-ready tables.

Example:

```text
mart_daily_orders
```

Possible fields:

```text
order_dt
created_orders_cnt
paid_orders_cnt
cancelled_orders_cnt
gross_order_amount
paid_revenue
orders_with_dq_cnt
```

dbt is now the primary analytics implementation: `make build-analytics` builds
the models in the `dbt` schema. The original SQL transformations remain as a
regression oracle and are not executed in the regular pipeline.

`make verify-dbt-parity` explicitly builds the legacy SQL tables and runs three
bidirectional comparisons for DDS, orphan-event DQ and MART. The dbt mart gets
affected old and new dates from the `dds_orders_history` snapshot, so a late
event that moves an order to another day cannot leave a stale aggregate behind.

---

## 9. Dead Letter Handling

Invalid events are not loaded into the raw business table.

They are saved into a dead-letter table:

```sql
CREATE TABLE IF NOT EXISTS dead_letter_events (
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    contract_name varchar NOT NULL,
    contract_version integer NOT NULL,
    raw_payload bytea,
    event_payload jsonb,
    error_message text NOT NULL,
    load_dttm timestamptz NOT NULL
);
```

`raw_payload` preserves the bytes received from Kafka, including malformed JSON
or invalid UTF-8. It is `NULL` for a Kafka tombstone or a legacy row created
before byte retention was introduced. `event_payload` contains parsed JSON when
parsing succeeded. The Kafka coordinates make DLQ insertion idempotent.

---

## 10. Idempotency

The ingestion process uses Kafka metadata as a technical key:

```text
kafka_topic + kafka_partition + kafka_offset
```

This prevents the same Kafka message from being loaded twice.

---

## 11. Local Run

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements-dbt.txt
```

Bootstrap infrastructure, topics and database objects:

```bash
make bootstrap
```

Produce a set of demo events:

```bash
make demo
```

Build the primary dbt analytics layer:

```bash
make build-analytics
```

Useful dbt-only commands:

```bash
make dbt-parse     # validate project structure without a database
make dbt-debug     # check the PostgreSQL connection
make dbt-build     # build dbt models, snapshot and non-parity tests
make dbt-source-freshness  # check the RAW load SLA
make verify-dbt-parity  # optionally build legacy SQL and verify equivalence
make dbt-docs      # generate the dbt catalog and documentation site
```

Run the end-to-end smoke test against the running stack:

```bash
make e2e
```

Run unit tests and contract compatibility checks:

```bash
make test-unit
make check-contracts
```

Run isolated PostgreSQL integration tests. The command starts PostgreSQL,
creates temporary databases with the `contract_dwh_it_` prefix, applies the
production migration script, and drops the databases after the test session:

```bash
make test-integration
```

Run the same complete sequence used by CI:

```bash
make ci
```

The CI workflow validates contracts, parses the dbt project, runs Python unit
tests, tests fresh and populated database migrations, exercises dbt
incrementality and parity, starts the full Redpanda/PostgreSQL stack, and
executes the RAW → DDS → MART smoke test. Docker status and logs are uploaded
as an artifact when a job fails.

Inspect the result:

```bash
make psql
```

Rebuild all DWH layers from the retained Kafka events:

```bash
make rebuild-from-kafka
```

Stop infrastructure:

```bash
make down
```

---

## 12. Orchestration and Observability

`make build-analytics` runs the observed analytics wrapper. It checks RAW
freshness, runs `dbt build`, reads dbt artifacts, and writes one audit row to
`analytics_run_history`.

Run it manually and inspect the history:

```bash
make analytics-run
make analytics-history
```

The default freshness SLA uses `raw_order_created.kafka_load_dttm` as a demo
ingestion heartbeat: 15 minutes produces a warning and 60 minutes produces an
error. Sparse payment and cancellation streams are intentionally excluded to
avoid false alerts when no business events occur. A freshness problem is
recorded as an analytics warning while the build still runs; use the strict
command when an SLA violation must fail the process:

```bash
make analytics-run-strict
```

Start the Docker Compose scheduler with a five-minute interval:

```bash
make orchestration-up
make orchestration-logs
```

Override the interval when needed:

```bash
make orchestration-up ANALYTICS_RUN_INTERVAL_SECONDS=60
```

Stop only the scheduler without stopping Kafka, PostgreSQL, or the consumer:

```bash
make orchestration-down
```

Concurrent executions are prevented by a PostgreSQL advisory lock. Set
`ALERT_WEBHOOK_URL` before `make orchestration-up` to send a JSON notification
when freshness warns/fails or the dbt build fails.

---

## 13. Example Mart

```sql
CREATE TABLE mart_daily_order_revenue AS
SELECT
    created_at::date AS order_date,
    currency,
    count(*) AS orders_cnt,
    sum(amount) AS revenue_amt
FROM raw_order_created
GROUP BY
    created_at::date,
    currency;
```

---

## 14. Compatibility Rules

Allowed changes:

* add a nullable field
* add a required field with a valid default
* add or expand a string enum
* add descriptions

Breaking changes:

* remove a field
* rename a field
* change field type
* make an existing nullable field non-nullable
* restrict or remove enum values
* change or remove an existing default
* change `key` or `quality.unique_key`
* add fields to `quality.not_null`
* change `allow_extra_fields` from `true` to `false`
* move a version chain to another topic namespace

Every evolution uses the next contiguous contract version:

```text
order_created.v1.yaml
order_created.v2.yaml
```

The checker treats the versions above as one compatibility chain and rejects a
breaking transition. A genuinely breaking event shape should use a new event
name/topic and be migrated explicitly instead of being presented as backward
compatible.

---

## 15. Data Quality Checks

The current pipeline validates required fields, types, enums and timestamp
timezones before RAW insertion. DDS also exposes business data quality flags:

```text
duplicate_created_events_cnt
dq_multiple_payments_flg
dq_multiple_cancellations_flg
dq_payment_amount_mismatch_flg
dq_payment_currency_mismatch_flg
dq_payment_before_creation_flg
dq_cancellation_before_creation_flg
dq_payment_after_cancellation_flg
```

Payments and cancellations without a matching `order_created` are exposed in
`dq_orphan_order_events`.

### Order status rules

`dds_orders.status` follows deterministic lifecycle precedence:

1. `cancelled` if at least one cancellation exists;
2. otherwise `paid` if at least one payment exists;
3. otherwise `created`.

Cancellation is terminal. A payment received after cancellation does not move
the order back to `paid`; it sets `dq_payment_after_cancellation_flg` instead.
The earliest creation event defines the order attributes, while the latest
payment and cancellation events provide their respective details.

Further useful checks:

* amount is non-negative
* refund amounts do not exceed captured payments
* business-key duplicates follow an explicit resolution policy
* RAW, DDS and MART freshness stay within their SLA

---

## 16. What This Project Demonstrates

This project demonstrates practical knowledge of:

* data contracts
* Kafka-compatible ingestion
* DWH layering
* raw, DDS and mart architecture
* schema validation
* formal contract meta-schema and semantic validation
* backward compatibility checks
* DDL generation
* contract provenance and payload retention in RAW/DLQ
* dead-letter handling
* idempotent loading
* data quality checks
* analytical mart design
* modular dbt transformations and lineage
* dbt data tests, unit tests and snapshots
* source freshness SLAs and dbt artifact parsing
* scheduled, concurrency-safe analytics execution
* persistent run history and webhook alerting
* safe side-by-side SQL-to-dbt parity validation
* database-backed integration and migration testing
* automated CI with end-to-end verification

---

## 17. Resume Description

Built a contract-driven DWH ingestion pipeline using Python, PostgreSQL,
Kafka-compatible streaming with Redpanda, and dbt. Implemented automatic RAW
DDL generation, event validation, dead-letter handling, idempotent ingestion,
incremental DDS and marts, historical snapshots, parity checks, database
integration tests, source freshness monitoring, scheduled dbt execution,
persistent run observability, and end-to-end CI.

---

## 18. Future Improvements

* add Schema Registry with Avro or Protobuf serialization
* integrate a production orchestrator such as Airflow or Dagster
* add historical DDS loading and a BI dashboard
* retain Kafka headers and producer/schema identifiers

---

## License

MIT
