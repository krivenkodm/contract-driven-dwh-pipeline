CREATE TABLE IF NOT EXISTS dds_orders (
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

TRUNCATE TABLE dds_orders;

WITH created_ranked AS (
    SELECT
        r.*,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY
                r.created_at,
                r.kafka_load_dttm,
                r.kafka_partition,
                r.kafka_offset
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS created_events_cnt

    FROM raw_order_created r
),

created AS (
    SELECT *
    FROM created_ranked
    WHERE rn = 1
),

payments_ranked AS (
    SELECT
        r.*,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY
                r.paid_at DESC,
                r.kafka_load_dttm DESC,
                r.kafka_partition DESC,
                r.kafka_offset DESC
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS payments_cnt

    FROM raw_order_paid r
),

payments AS (
    SELECT *
    FROM payments_ranked
    WHERE rn = 1
),

cancellations_ranked AS (
    SELECT
        r.*,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id
            ORDER BY
                r.cancelled_at DESC,
                r.kafka_load_dttm DESC,
                r.kafka_partition DESC,
                r.kafka_offset DESC
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS cancellations_cnt

    FROM raw_order_cancelled r
),

cancellations AS (
    SELECT *
    FROM cancellations_ranked
    WHERE rn = 1
)

INSERT INTO dds_orders (
    order_id,
    customer_id,
    order_amount,
    currency,
    created_at,
    status,
    payment_id,
    paid_amount,
    paid_at,
    cancellation_id,
    cancellation_reason,
    cancelled_at,
    payments_cnt,
    cancellations_cnt,
    duplicate_created_events_cnt,
    dq_multiple_payments_flg,
    dq_multiple_cancellations_flg,
    processed_dttm
)

SELECT
    cr.order_id,
    cr.customer_id,
    cr.amount AS order_amount,
    cr.currency,
    cr.created_at,

    CASE
        WHEN p.paid_at IS NULL
             AND ca.cancelled_at IS NULL
            THEN 'created'

        WHEN ca.cancelled_at IS NULL
            THEN 'paid'

        WHEN p.paid_at IS NULL
            THEN 'cancelled'

        WHEN ca.cancelled_at >= p.paid_at
            THEN 'cancelled'

        ELSE 'paid'
    END AS status,

    p.payment_id,
    p.paid_amount,
    p.paid_at,

    ca.cancellation_id,
    ca.cancellation_reason,
    ca.cancelled_at,

    COALESCE(p.payments_cnt, 0)::integer
        AS payments_cnt,

    COALESCE(ca.cancellations_cnt, 0)::integer
        AS cancellations_cnt,

    GREATEST(
        cr.created_events_cnt - 1,
        0
    )::integer AS duplicate_created_events_cnt,

    COALESCE(p.payments_cnt, 0) > 1
        AS dq_multiple_payments_flg,

    COALESCE(ca.cancellations_cnt, 0) > 1
        AS dq_multiple_cancellations_flg,

    CURRENT_TIMESTAMP AS processed_dttm

FROM created cr

LEFT JOIN payments p
    ON p.order_id = cr.order_id

LEFT JOIN cancellations ca
    ON ca.order_id = cr.order_id;