-- Existing timestamp values were produced in UTC. Setting the session
-- timezone makes their conversion to timestamptz deterministic.
SET LOCAL TIME ZONE 'UTC';


ALTER TABLE raw_order_created
    ALTER COLUMN kafka_load_dttm
        TYPE timestamptz
        USING kafka_load_dttm::timestamptz,
    ALTER COLUMN created_at
        TYPE timestamptz
        USING created_at::timestamptz;


ALTER TABLE raw_order_paid
    ALTER COLUMN kafka_load_dttm
        TYPE timestamptz
        USING kafka_load_dttm::timestamptz,
    ALTER COLUMN paid_at
        TYPE timestamptz
        USING paid_at::timestamptz;


ALTER TABLE raw_order_cancelled
    ALTER COLUMN kafka_load_dttm
        TYPE timestamptz
        USING kafka_load_dttm::timestamptz,
    ALTER COLUMN cancelled_at
        TYPE timestamptz
        USING cancelled_at::timestamptz;


ALTER TABLE dead_letter_events
    ALTER COLUMN load_dttm
        TYPE timestamptz
        USING load_dttm::timestamptz;


ALTER TABLE dds_orders
    ALTER COLUMN created_at
        TYPE timestamptz
        USING created_at::timestamptz,
    ALTER COLUMN paid_at
        TYPE timestamptz
        USING paid_at::timestamptz,
    ALTER COLUMN cancelled_at
        TYPE timestamptz
        USING cancelled_at::timestamptz,
    ALTER COLUMN processed_dttm
        TYPE timestamptz
        USING processed_dttm::timestamptz;


ALTER TABLE etl_watermarks
    ALTER COLUMN processed_dttm
        TYPE timestamptz
        USING processed_dttm::timestamptz;


ALTER TABLE mart_daily_orders
    ALTER COLUMN processed_dttm
        TYPE timestamptz
        USING processed_dttm::timestamptz;


ALTER TABLE mart_watermarks
    ALTER COLUMN processed_dttm
        TYPE timestamptz
        USING processed_dttm::timestamptz;


ALTER TABLE schema_migrations
    ALTER COLUMN applied_dttm
        TYPE timestamptz
        USING applied_dttm::timestamptz;
