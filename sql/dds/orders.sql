BEGIN;


CREATE SEQUENCE IF NOT EXISTS dds_orders_change_id_seq;


CREATE TABLE IF NOT EXISTS dds_orders (
    dds_change_id bigint NOT NULL
        DEFAULT nextval('dds_orders_change_id_seq'),

    order_id varchar PRIMARY KEY,

    customer_id varchar NOT NULL,
    source_channel varchar,
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


-- Миграция уже существующей таблицы.
CREATE INDEX IF NOT EXISTS ix_dds_orders_change_id
    ON dds_orders (dds_change_id);


ALTER TABLE dds_orders
    ADD COLUMN IF NOT EXISTS source_channel varchar;


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
        ) AS payments_cnt

    FROM raw_order_paid r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_paid'

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
        ) AS cancellations_cnt

    FROM raw_order_cancelled r

    INNER JOIN tmp_dds_orders_affected a
        ON a.order_id = r.order_id

    INNER JOIN tmp_dds_orders_high_watermarks h
        ON h.source_table = 'raw_order_cancelled'

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
    order_id,
    customer_id,
    source_channel,
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

    processed_dttm =
        EXCLUDED.processed_dttm;


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