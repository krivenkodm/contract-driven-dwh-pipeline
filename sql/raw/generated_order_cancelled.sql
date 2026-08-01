-- Generated from order_cancelled v1
CREATE TABLE IF NOT EXISTS raw_order_cancelled (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_load_dttm timestamptz NOT NULL,
    contract_name varchar NOT NULL,
    contract_version integer NOT NULL,
    original_payload jsonb NOT NULL,
    order_id varchar NOT NULL,
    cancellation_id varchar NOT NULL,
    cancellation_reason varchar NOT NULL,
    cancelled_at timestamptz NOT NULL,
    CONSTRAINT uq_raw_order_cancelled_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
