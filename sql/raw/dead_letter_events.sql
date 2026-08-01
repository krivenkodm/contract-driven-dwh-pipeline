CREATE TABLE IF NOT EXISTS dead_letter_events (
    id                  bigserial PRIMARY KEY,
    kafka_topic         varchar NOT NULL,
    kafka_partition     integer NOT NULL,
    kafka_offset        bigint NOT NULL,
    event_payload       jsonb,
    error_message       text NOT NULL,
    load_dttm           timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_dead_letter_kafka_message
        UNIQUE (
            kafka_topic,
            kafka_partition,
            kafka_offset
        )
);
