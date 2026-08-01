CREATE SEQUENCE IF NOT EXISTS dds_orders_change_id_seq;


CREATE TABLE IF NOT EXISTS dds_orders (
    dds_change_id bigint NOT NULL
        DEFAULT nextval('dds_orders_change_id_seq'),

    order_id varchar PRIMARY KEY,

    customer_id varchar NOT NULL,
    order_amount numeric(12, 2) NOT NULL,
    currency varchar NOT NULL,
    created_at timestamp NOT NULL,

    status varchar NOT NULL,

    payment_id varchar,
    paid_amount numeric(12, 2),
    paid_at timestamp,

    cancellation_id varchar,
    cancellation_reason varchar,
    cancelled_at timestamp,

    payments_cnt integer NOT NULL,
    cancellations_cnt integer NOT NULL,
    duplicate_created_events_cnt integer NOT NULL,

    dq_multiple_payments_flg boolean NOT NULL,
    dq_multiple_cancellations_flg boolean NOT NULL,

    processed_dttm timestamp NOT NULL
);


ALTER SEQUENCE dds_orders_change_id_seq
    OWNED BY dds_orders.dds_change_id;


CREATE INDEX IF NOT EXISTS ix_dds_orders_change_id
    ON dds_orders (dds_change_id);


CREATE TABLE IF NOT EXISTS etl_watermarks (
    pipeline_name varchar NOT NULL,
    source_table varchar NOT NULL,
    last_raw_id bigint NOT NULL,
    processed_dttm timestamp NOT NULL,

    PRIMARY KEY (
        pipeline_name,
        source_table
    )
);