CREATE TABLE IF NOT EXISTS mart_daily_orders (
    order_dt date PRIMARY KEY,

    created_orders_cnt integer NOT NULL,
    paid_orders_cnt integer NOT NULL,
    cancelled_orders_cnt integer NOT NULL,
    created_status_orders_cnt integer NOT NULL,

    gross_order_amount numeric(14, 2) NOT NULL,
    paid_revenue numeric(14, 2) NOT NULL,

    orders_with_dq_cnt integer NOT NULL,

    processed_dttm timestamp NOT NULL
);

TRUNCATE TABLE mart_daily_orders;

INSERT INTO mart_daily_orders (
    order_dt,
    created_orders_cnt,
    paid_orders_cnt,
    cancelled_orders_cnt,
    created_status_orders_cnt,
    gross_order_amount,
    paid_revenue,
    orders_with_dq_cnt,
    processed_dttm
)

SELECT
    created_at::date AS order_dt,

    COUNT(*)::integer AS created_orders_cnt,

    COUNT(*) FILTER (
        WHERE status = 'paid'
    )::integer AS paid_orders_cnt,

    COUNT(*) FILTER (
        WHERE status = 'cancelled'
    )::integer AS cancelled_orders_cnt,

    COUNT(*) FILTER (
        WHERE status = 'created'
    )::integer AS created_status_orders_cnt,

    COALESCE(
        SUM(order_amount),
        0
    )::numeric(14, 2) AS gross_order_amount,

    COALESCE(
        SUM(paid_amount) FILTER (
            WHERE status = 'paid'
        ),
        0
    )::numeric(14, 2) AS paid_revenue,

    COUNT(*) FILTER (
        WHERE duplicate_created_events_cnt > 0
           OR dq_multiple_payments_flg
           OR dq_multiple_cancellations_flg
    )::integer AS orders_with_dq_cnt,

    CURRENT_TIMESTAMP AS processed_dttm

FROM dds_orders

GROUP BY
    created_at::date;