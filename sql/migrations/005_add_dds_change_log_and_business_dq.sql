ALTER TABLE dds_orders
    ADD COLUMN payment_currency varchar,
    ADD COLUMN dq_payment_amount_mismatch_flg boolean NOT NULL
        DEFAULT FALSE,
    ADD COLUMN dq_payment_currency_mismatch_flg boolean NOT NULL
        DEFAULT FALSE,
    ADD COLUMN dq_payment_before_creation_flg boolean NOT NULL
        DEFAULT FALSE,
    ADD COLUMN dq_cancellation_before_creation_flg boolean NOT NULL
        DEFAULT FALSE,
    ADD COLUMN dq_payment_after_cancellation_flg boolean NOT NULL
        DEFAULT FALSE;


ALTER TABLE dds_orders
    ADD CONSTRAINT ck_dds_orders_status
        CHECK (
            status IN (
                'created',
                'paid',
                'cancelled'
            )
        );


CREATE TABLE dds_orders_changes (
    change_id bigint
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    order_id varchar NOT NULL,
    old_order_dt date,
    new_order_dt date,
    dds_change_id bigint NOT NULL,

    changed_dttm timestamptz NOT NULL
        DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX ix_dds_orders_changes_order_id
    ON dds_orders_changes (order_id);


CREATE OR REPLACE FUNCTION record_dds_orders_change()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO dds_orders_changes (
        order_id,
        old_order_dt,
        new_order_dt,
        dds_change_id,
        changed_dttm
    )
    VALUES (
        NEW.order_id,
        CASE
            WHEN TG_OP = 'UPDATE'
                THEN OLD.created_at::date
            ELSE NULL
        END,
        NEW.created_at::date,
        NEW.dds_change_id,
        CURRENT_TIMESTAMP
    );

    RETURN NEW;
END;
$$;


CREATE TRIGGER trg_dds_orders_change
AFTER INSERT OR UPDATE ON dds_orders
FOR EACH ROW
EXECUTE FUNCTION record_dds_orders_change();


INSERT INTO dds_orders_changes (
    order_id,
    old_order_dt,
    new_order_dt,
    dds_change_id,
    changed_dttm
)
SELECT
    order_id,
    NULL,
    created_at::date,
    dds_change_id,
    CURRENT_TIMESTAMP

FROM dds_orders

ORDER BY
    order_id;


CREATE TABLE dq_orphan_order_events (
    order_id varchar PRIMARY KEY,

    payments_cnt integer NOT NULL,
    cancellations_cnt integer NOT NULL,

    first_event_at timestamptz NOT NULL,
    last_event_at timestamptz NOT NULL,

    processed_dttm timestamptz NOT NULL
);


WITH orphan_events AS (
    SELECT
        order_id,
        'payment'::varchar AS event_type,
        paid_at AS event_at

    FROM raw_order_paid

    UNION ALL

    SELECT
        order_id,
        'cancellation'::varchar AS event_type,
        cancelled_at AS event_at

    FROM raw_order_cancelled
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

WHERE NOT EXISTS (
    SELECT 1

    FROM raw_order_created c

    WHERE
        c.order_id = e.order_id
)

GROUP BY
    e.order_id;


ALTER TABLE mart_watermarks
    RENAME COLUMN last_dds_change_id
    TO last_change_id;


UPDATE mart_watermarks
SET
    last_change_id = 0,
    processed_dttm = CURRENT_TIMESTAMP
WHERE
    pipeline_name = 'mart_daily_orders';
