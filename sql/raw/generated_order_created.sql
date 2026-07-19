CREATE TABLE IF NOT EXISTS raw_order_created (
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_load_dttm timestamp NOT NULL,
    order_id varchar NOT NULL,
    customer_id varchar NOT NULL,
    amount numeric(12,2) NOT NULL,
    currency varchar NOT NULL,
    created_at timestamp NOT NULL,
    CONSTRAINT uq_raw_order_created_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
