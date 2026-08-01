BEGIN;


SELECT pg_advisory_xact_lock(
    hashtextextended('mart_daily_orders', 0)
);


CREATE TEMP TABLE tmp_mart_daily_orders_high_watermark
ON COMMIT DROP
AS
SELECT
    COALESCE(
        MAX(dds_change_id),
        0
    )::bigint AS high_dds_change_id

FROM dds_orders;


CREATE TEMP TABLE tmp_mart_daily_orders_affected_dates
ON COMMIT DROP
AS
SELECT DISTINCT
    d.created_at::date AS order_dt

FROM dds_orders d

CROSS JOIN tmp_mart_daily_orders_high_watermark h

LEFT JOIN mart_watermarks w
    ON w.pipeline_name = 'mart_daily_orders'

WHERE
    d.dds_change_id > COALESCE(
        w.last_dds_change_id,
        0
    )

    AND d.dds_change_id <=
        h.high_dds_change_id;


SELECT
    COUNT(*) AS affected_order_dates_cnt

FROM tmp_mart_daily_orders_affected_dates;


WITH aggregated_dates AS (
    SELECT
        d.created_at::date AS order_dt,

        COUNT(*)::integer
            AS created_orders_cnt,

        COUNT(*) FILTER (
            WHERE d.status = 'paid'
        )::integer AS paid_orders_cnt,

        COUNT(*) FILTER (
            WHERE d.status = 'cancelled'
        )::integer AS cancelled_orders_cnt,

        COUNT(*) FILTER (
            WHERE d.status = 'created'
        )::integer AS created_status_orders_cnt,

        COALESCE(
            SUM(d.order_amount),
            0
        )::numeric(14, 2)
            AS gross_order_amount,

        COALESCE(
            SUM(d.paid_amount) FILTER (
                WHERE d.status = 'paid'
            ),
            0
        )::numeric(14, 2)
            AS paid_revenue,

        COUNT(*) FILTER (
            WHERE
                d.duplicate_created_events_cnt > 0
                OR d.dq_multiple_payments_flg
                OR d.dq_multiple_cancellations_flg
        )::integer AS orders_with_dq_cnt,

        CURRENT_TIMESTAMP AS processed_dttm

    FROM dds_orders d

    INNER JOIN tmp_mart_daily_orders_affected_dates a
        ON a.order_dt = d.created_at::date

    GROUP BY
        d.created_at::date
)

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
    order_dt,
    created_orders_cnt,
    paid_orders_cnt,
    cancelled_orders_cnt,
    created_status_orders_cnt,
    gross_order_amount,
    paid_revenue,
    orders_with_dq_cnt,
    processed_dttm

FROM aggregated_dates

ON CONFLICT (order_dt)
DO UPDATE SET
    created_orders_cnt =
        EXCLUDED.created_orders_cnt,

    paid_orders_cnt =
        EXCLUDED.paid_orders_cnt,

    cancelled_orders_cnt =
        EXCLUDED.cancelled_orders_cnt,

    created_status_orders_cnt =
        EXCLUDED.created_status_orders_cnt,

    gross_order_amount =
        EXCLUDED.gross_order_amount,

    paid_revenue =
        EXCLUDED.paid_revenue,

    orders_with_dq_cnt =
        EXCLUDED.orders_with_dq_cnt,

    processed_dttm =
        EXCLUDED.processed_dttm;


INSERT INTO mart_watermarks (
    pipeline_name,
    last_dds_change_id,
    processed_dttm
)

SELECT
    'mart_daily_orders',
    high_dds_change_id,
    CURRENT_TIMESTAMP

FROM tmp_mart_daily_orders_high_watermark

ON CONFLICT (pipeline_name)
DO UPDATE SET
    last_dds_change_id = GREATEST(
        mart_watermarks.last_dds_change_id,
        EXCLUDED.last_dds_change_id
    ),

    processed_dttm =
        EXCLUDED.processed_dttm;


COMMIT;