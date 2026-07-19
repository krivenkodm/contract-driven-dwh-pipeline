CREATE TABLE IF NOT EXISTS raw_order_cancelled (
    kafka_topic varchar NOT NULL,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    kafka_load_dttm timestamp NOT NULL,
    order_id varchar NOT NULL,
    cancellation_id varchar NOT NULL,
    cancellation_reason varchar NOT NULL,
    cancelled_at timestamp NOT NULL,
    CONSTRAINT uq_raw_order_cancelled_kafka_message UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);
