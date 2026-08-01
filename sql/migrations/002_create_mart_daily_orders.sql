CREATE TABLE IF NOT EXISTS mart_daily_orders (
    order_dt date PRIMARY KEY,

    created_orders_cnt bigint NOT NULL,
    paid_orders_cnt bigint NOT NULL,
    cancelled_orders_cnt bigint NOT NULL,
    created_status_orders_cnt bigint NOT NULL,

    gross_order_amount numeric(18, 2) NOT NULL,
    paid_revenue numeric(18, 2) NOT NULL,

    orders_with_dq_cnt bigint NOT NULL,

    processed_dttm timestamp NOT NULL
);


CREATE TABLE IF NOT EXISTS mart_watermarks (
    pipeline_name varchar PRIMARY KEY,
    last_dds_change_id bigint NOT NULL,
    processed_dttm timestamp NOT NULL
);