BEGIN;


SET LOCAL TIME ZONE 'UTC';


SELECT pg_advisory_xact_lock(
    hashtextextended('dds_orders', 0)
);


CREATE TEMP TABLE tmp_dds_orders_high_watermarks
ON COMMIT DROP
AS
SELECT
    'raw_order_created'::varchar AS source_table,

    COALESCE(
        MAX(raw_id),
        0
    )::bigint AS high_raw_id

FROM raw_order_created

UNION ALL

SELECT
    'raw_order_paid'::varchar AS source_table,

    COALESCE(
        MAX(raw_id),
        0
    )::bigint AS high_raw_id

FROM raw_order_paid

UNION ALL

SELECT
    'raw_order_cancelled'::varchar AS source_table,

    COALESCE(
        MAX(raw_id),
        0
    )::bigint AS high_raw_id

FROM raw_order_cancelled;


CREATE TEMP TABLE tmp_dds_orders_affected
ON COMMIT DROP
AS
SELECT DISTINCT
    changed.order_id

FROM (
    SELECT
        r.order_id

    FROM raw_order_created r

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_created'

    LEFT JOIN etl_watermarks w
        ON w.pipeline_name = 'dds_orders'
       AND w.source_table = 'raw_order_created'

    WHERE
        r.raw_id > COALESCE(
            w.last_raw_id,
            0
        )

        AND r.raw_id <= h.high_raw_id

    UNION ALL

    SELECT
        r.order_id

    FROM raw_order_paid r

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_paid'

    LEFT JOIN etl_watermarks w
        ON w.pipeline_name = 'dds_orders'
       AND w.source_table = 'raw_order_paid'

    WHERE
        r.raw_id > COALESCE(
            w.last_raw_id,
            0
        )

        AND r.raw_id <= h.high_raw_id

    UNION ALL

    SELECT
        r.order_id

    FROM raw_order_cancelled r

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_cancelled'

    LEFT JOIN etl_watermarks w
        ON w.pipeline_name = 'dds_orders'
       AND w.source_table = 'raw_order_cancelled'

    WHERE
        r.raw_id > COALESCE(
            w.last_raw_id,
            0
        )

        AND r.raw_id <= h.high_raw_id
) changed;


SELECT
    COUNT(*) AS affected_orders_cnt

FROM tmp_dds_orders_affected;


WITH created_ranked AS (
    SELECT
        r.*,

        ROW_NUMBER() OVER (
            PARTITION BY r.order_id

            ORDER BY
                r.created_at,
                r.kafka_load_dttm,
                r.kafka_partition,
                r.kafka_offset,
                r.raw_id
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS created_events_cnt

    FROM raw_order_created r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_created'

    WHERE
        r.raw_id <= h.high_raw_id
),

created AS (
    SELECT
        *

    FROM created_ranked

    WHERE
        rn = 1
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
                r.kafka_offset DESC,
                r.raw_id DESC
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS payments_cnt,

        BOOL_OR(
            r.paid_amount <> cr.amount
        ) OVER (
            PARTITION BY r.order_id
        ) AS dq_payment_amount_mismatch_flg,

        BOOL_OR(
            r.currency <> cr.currency
        ) OVER (
            PARTITION BY r.order_id
        ) AS dq_payment_currency_mismatch_flg,

        BOOL_OR(
            r.paid_at < cr.created_at
        ) OVER (
            PARTITION BY r.order_id
        ) AS dq_payment_before_creation_flg

    FROM raw_order_paid r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_paid'

    INNER JOIN created cr
        ON cr.order_id = r.order_id

    WHERE
        r.raw_id <= h.high_raw_id
),

payments AS (
    SELECT
        *

    FROM payments_ranked

    WHERE
        rn = 1
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
                r.kafka_offset DESC,
                r.raw_id DESC
        ) AS rn,

        COUNT(*) OVER (
            PARTITION BY r.order_id
        ) AS cancellations_cnt,

        MIN(r.cancelled_at) OVER (
            PARTITION BY r.order_id
        ) AS first_cancelled_at,

        BOOL_OR(
            r.cancelled_at < cr.created_at
        ) OVER (
            PARTITION BY r.order_id
        ) AS dq_cancellation_before_creation_flg

    FROM raw_order_cancelled r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_cancelled'

    INNER JOIN created cr
        ON cr.order_id = r.order_id

    WHERE
        r.raw_id <= h.high_raw_id
),

cancellations AS (
    SELECT
        *

    FROM cancellations_ranked

    WHERE
        rn = 1
),

prepared_orders AS (
    SELECT
        cr.order_id,
        cr.customer_id,
        cr.source_channel,
        cr.amount AS order_amount,
        cr.currency,
        cr.created_at,

        CASE
            WHEN ca.cancelled_at IS NOT NULL
                THEN 'cancelled'

            WHEN p.paid_at IS NOT NULL
                THEN 'paid'

            ELSE 'created'
        END AS status,

        p.payment_id,
        p.paid_amount,
        p.currency AS payment_currency,
        p.paid_at,

        ca.cancellation_id,
        ca.cancellation_reason,
        ca.cancelled_at,

        COALESCE(
            p.payments_cnt,
            0
        )::integer AS payments_cnt,

        COALESCE(
            ca.cancellations_cnt,
            0
        )::integer AS cancellations_cnt,

        GREATEST(
            cr.created_events_cnt - 1,
            0
        )::integer AS duplicate_created_events_cnt,

        COALESCE(
            p.payments_cnt,
            0
        ) > 1 AS dq_multiple_payments_flg,

        COALESCE(
            ca.cancellations_cnt,
            0
        ) > 1 AS dq_multiple_cancellations_flg,

        COALESCE(
            p.dq_payment_amount_mismatch_flg,
            FALSE
        ) AS dq_payment_amount_mismatch_flg,

        COALESCE(
            p.dq_payment_currency_mismatch_flg,
            FALSE
        ) AS dq_payment_currency_mismatch_flg,

        COALESCE(
            p.dq_payment_before_creation_flg,
            FALSE
        ) AS dq_payment_before_creation_flg,

        COALESCE(
            ca.dq_cancellation_before_creation_flg,
            FALSE
        ) AS dq_cancellation_before_creation_flg,

        (
            p.paid_at IS NOT NULL
            AND ca.first_cancelled_at IS NOT NULL
            AND p.paid_at > ca.first_cancelled_at
        ) AS dq_payment_after_cancellation_flg,

        CURRENT_TIMESTAMP AS processed_dttm

    FROM created cr

    LEFT JOIN payments p
        ON p.order_id = cr.order_id

    LEFT JOIN cancellations ca
        ON ca.order_id = cr.order_id
)


INSERT INTO dds_orders (
    order_id,
    customer_id,
    source_channel,
    order_amount,
    currency,
    created_at,
    status,
    payment_id,
    paid_amount,
    payment_currency,
    paid_at,
    cancellation_id,
    cancellation_reason,
    cancelled_at,
    payments_cnt,
    cancellations_cnt,
    duplicate_created_events_cnt,
    dq_multiple_payments_flg,
    dq_multiple_cancellations_flg,
    dq_payment_amount_mismatch_flg,
    dq_payment_currency_mismatch_flg,
    dq_payment_before_creation_flg,
    dq_cancellation_before_creation_flg,
    dq_payment_after_cancellation_flg,
    processed_dttm
)

SELECT
    order_id,
    customer_id,
    source_channel,
    order_amount,
    currency,
    created_at,
    status,
    payment_id,
    paid_amount,
    payment_currency,
    paid_at,
    cancellation_id,
    cancellation_reason,
    cancelled_at,
    payments_cnt,
    cancellations_cnt,
    duplicate_created_events_cnt,
    dq_multiple_payments_flg,
    dq_multiple_cancellations_flg,
    dq_payment_amount_mismatch_flg,
    dq_payment_currency_mismatch_flg,
    dq_payment_before_creation_flg,
    dq_cancellation_before_creation_flg,
    dq_payment_after_cancellation_flg,
    processed_dttm

FROM prepared_orders

ON CONFLICT (order_id)
DO UPDATE SET
    dds_change_id =
        nextval('dds_orders_change_id_seq'),

    customer_id =
        EXCLUDED.customer_id,

    source_channel =
        EXCLUDED.source_channel,

    order_amount =
        EXCLUDED.order_amount,

    currency =
        EXCLUDED.currency,

    created_at =
        EXCLUDED.created_at,

    status =
        EXCLUDED.status,

    payment_id =
        EXCLUDED.payment_id,

    paid_amount =
        EXCLUDED.paid_amount,

    payment_currency =
        EXCLUDED.payment_currency,

    paid_at =
        EXCLUDED.paid_at,

    cancellation_id =
        EXCLUDED.cancellation_id,

    cancellation_reason =
        EXCLUDED.cancellation_reason,

    cancelled_at =
        EXCLUDED.cancelled_at,

    payments_cnt =
        EXCLUDED.payments_cnt,

    cancellations_cnt =
        EXCLUDED.cancellations_cnt,

    duplicate_created_events_cnt =
        EXCLUDED.duplicate_created_events_cnt,

    dq_multiple_payments_flg =
        EXCLUDED.dq_multiple_payments_flg,

    dq_multiple_cancellations_flg =
        EXCLUDED.dq_multiple_cancellations_flg,

    dq_payment_amount_mismatch_flg =
        EXCLUDED.dq_payment_amount_mismatch_flg,

    dq_payment_currency_mismatch_flg =
        EXCLUDED.dq_payment_currency_mismatch_flg,

    dq_payment_before_creation_flg =
        EXCLUDED.dq_payment_before_creation_flg,

    dq_cancellation_before_creation_flg =
        EXCLUDED.dq_cancellation_before_creation_flg,

    dq_payment_after_cancellation_flg =
        EXCLUDED.dq_payment_after_cancellation_flg,

    processed_dttm =
        EXCLUDED.processed_dttm;


DELETE FROM dq_orphan_order_events q
USING tmp_dds_orders_affected a
WHERE
    q.order_id = a.order_id;


WITH orphan_events AS (
    SELECT
        r.order_id,
        'payment'::varchar AS event_type,
        r.paid_at AS event_at

    FROM raw_order_paid r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_paid'

    WHERE
        r.raw_id <= h.high_raw_id

    UNION ALL

    SELECT
        r.order_id,
        'cancellation'::varchar AS event_type,
        r.cancelled_at AS event_at

    FROM raw_order_cancelled r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_cancelled'

    WHERE
        r.raw_id <= h.high_raw_id
),

created_order_ids AS (
    SELECT DISTINCT
        r.order_id

    FROM raw_order_created r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_created'

    WHERE
        r.raw_id <= h.high_raw_id
)

INSERT INTO dq_orphan_order_events (
    order_id,
    payments_cnt,
    cancellations_cnt,
    first_event_at,
    last_event_at,
    processed_dttm
)

SELECT
    e.order_id,

    COUNT(*) FILTER (
        WHERE e.event_type = 'payment'
    )::integer AS payments_cnt,

    COUNT(*) FILTER (
        WHERE e.event_type = 'cancellation'
    )::integer AS cancellations_cnt,

    MIN(e.event_at) AS first_event_at,
    MAX(e.event_at) AS last_event_at,
    CURRENT_TIMESTAMP AS processed_dttm

FROM orphan_events e

LEFT JOIN created_order_ids c
    ON c.order_id = e.order_id

WHERE
    c.order_id IS NULL

GROUP BY
    e.order_id;


INSERT INTO etl_watermarks (
    pipeline_name,
    source_table,
    last_raw_id,
    processed_dttm
)

SELECT
    'dds_orders' AS pipeline_name,
    source_table,
    high_raw_id,
    CURRENT_TIMESTAMP AS processed_dttm

FROM tmp_dds_orders_high_watermarks

ON CONFLICT (
    pipeline_name,
    source_table
)
DO UPDATE SET
    last_raw_id = GREATEST(
        etl_watermarks.last_raw_id,
        EXCLUDED.last_raw_id
    ),

    processed_dttm =
        EXCLUDED.processed_dttm;


COMMIT;
