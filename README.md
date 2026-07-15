# Contract-Driven DWH Pipeline

Demo project showing how **data contracts** can drive a DWH ingestion pipeline from Kafka-compatible streaming to analytical marts.

The project uses **Redpanda** as a lightweight Kafka-compatible broker for local development.

```text
Data Contract → Kafka-compatible Topic → Event Validation → Raw DWH → DDS → Mart
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
* build DDS and mart tables
* run basic data quality checks

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
┌─────────────────┐
│ Raw DWH Layer   │
└────────┬────────┘
         v
┌─────────────────┐
│ DDS Layer       │
└────────┬────────┘
         v
┌─────────────────┐
│ Mart Layer      │
└─────────────────┘
```

---

## 3. Tech Stack

* Python
* Redpanda as a Kafka-compatible broker
* PostgreSQL as a demo DWH
* YAML data contracts
* Docker Compose
* pytest
* Makefile

---

## 4. Repository Structure

```text
contract-driven-dwh-pipeline/
│
├── contracts/
│   ├── order_created.v1.yaml
│   └── order_paid.v1.yaml
│
├── src/
│   ├── contract_loader.py
│   ├── ddl_generator.py
│   ├── validator.py
│   ├── producer.py
│   ├── consumer.py
│   └── dwh_writer.py
│
├── sql/
│   ├── raw/
│   ├── dds/
│   └── mart/
│
├── tests/
│   ├── test_contract_validation.py
│   ├── test_ddl_generation.py
│   └── test_event_validation.py
│
├── docker-compose.yml
├── Makefile
├── requirements.txt
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
  "created_at": "2026-07-15T12:00:00"
}
```

---

## 7. Generated Raw Table

The raw table is generated from the contract.

```sql
CREATE TABLE IF NOT EXISTS raw_order_created (
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    kafka_load_dttm timestamp,

    order_id varchar NOT NULL,
    customer_id varchar NOT NULL,
    amount numeric(12,2) NOT NULL,
    currency varchar NOT NULL,
    created_at timestamp NOT NULL
);
```

The raw layer stores source events with Kafka metadata.

---

## 8. DWH Layers

### Raw

Stores source events almost as they arrived.

Purpose:

* preserve original data
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
mart_daily_order_revenue
```

Possible fields:

```text
order_date
currency
orders_cnt
paid_orders_cnt
revenue_amt
payment_conversion_pct
```

---

## 9. Dead Letter Handling

Invalid events are not loaded into the raw business table.

They are saved into a dead-letter table:

```sql
CREATE TABLE IF NOT EXISTS dead_letter_events (
    kafka_topic varchar,
    kafka_partition integer,
    kafka_offset bigint,
    event_payload jsonb,
    error_message text,
    load_dttm timestamp
);
```

This allows the pipeline to keep processing valid events while preserving invalid ones for debugging.

---

## 10. Idempotency

The ingestion process uses Kafka metadata as a technical key:

```text
kafka_topic + kafka_partition + kafka_offset
```

This prevents the same Kafka message from being loaded twice.

---

## 11. Local Run

Start infrastructure:

```bash
make up
```

Generate raw DWH DDL:

```bash
make generate-ddl
```

Initialize database:

```bash
make init-db
```

Produce demo events:

```bash
make produce-orders
```

Consume events and load them into PostgreSQL:

```bash
make consume-orders
```

Build DDS and mart tables:

```bash
make build-marts
```

Run data quality checks:

```bash
make dq
```

Run tests:

```bash
make test
```

Stop infrastructure:

```bash
make down
```

---

## 12. Example Mart

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

## 13. Compatibility Rules

Allowed changes:

* add a nullable field
* add metadata or descriptions
* add a new enum value if downstream logic supports it

Breaking changes:

* remove a field
* rename a field
* change field type
* make a nullable field non-nullable without a default value
* change event semantics without creating a new version

For breaking changes, create a new contract version:

```text
order_created.v1.yaml
order_created.v2.yaml
```

---

## 14. Data Quality Checks

Example checks:

```sql
SELECT count(*)
FROM raw_order_created
WHERE order_id IS NULL;
```

```sql
SELECT order_id, count(*)
FROM raw_order_created
GROUP BY order_id
HAVING count(*) > 1;
```

Possible checks:

* required fields are not null
* business key is unique
* enum values are valid
* amount is non-negative
* event time is valid
* Kafka offsets are not duplicated

---

## 15. What This Project Demonstrates

This project demonstrates practical knowledge of:

* data contracts
* Kafka-compatible ingestion
* DWH layering
* raw, DDS and mart architecture
* schema validation
* DDL generation
* dead-letter handling
* idempotent loading
* data quality checks
* analytical mart design

---

## 16. Resume Description

Built a contract-driven DWH ingestion pipeline using Python, PostgreSQL and Kafka-compatible streaming with Redpanda. Implemented automatic raw DWH DDL generation, event validation, dead-letter handling, idempotent ingestion and analytical mart creation.

---

## 17. Future Improvements

* add Schema Registry
* add Avro or Protobuf serialization
* add dbt transformations
* add CI checks for contract compatibility
* add more events: `order_paid`, `order_cancelled`
* add historical DDS loading
* add a simple BI dashboard

---

## License

MIT
