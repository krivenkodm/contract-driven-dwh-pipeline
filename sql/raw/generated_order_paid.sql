-- Generated from order_paid v1
CREATE TABLE IF NOT EXISTS raw_order_paid (
    raw_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_load_dttm timestamptz NOT NULL,
    order_id varchar NOT NULL,
    payment_id varchar NOT NULL,
    paid_amount numeric(12,2) NOT NULL,
    currency varchar NOT NULL,
    paid_at timestamptz NOT NULL,
    CONSTRAINT uq_raw_order_paid_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
