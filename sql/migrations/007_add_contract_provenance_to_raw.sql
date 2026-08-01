ALTER TABLE raw_order_created
    ADD COLUMN IF NOT EXISTS contract_name varchar,
    ADD COLUMN IF NOT EXISTS contract_version integer,
    ADD COLUMN IF NOT EXISTS original_payload jsonb;


UPDATE raw_order_created
SET
    contract_name = 'order_created',
    contract_version = COALESCE(
        substring(
            kafka_topic
            FROM '\.v([0-9]+)$'
        )::integer,
        1
    ),
    original_payload = jsonb_strip_nulls(
        jsonb_build_object(
            'order_id', order_id,
            'customer_id', customer_id,
            'amount', amount,
            'currency', currency,
            'created_at', created_at,
            'source_channel', source_channel
        )
    )
WHERE
    contract_name IS NULL
    OR contract_version IS NULL
    OR original_payload IS NULL;


ALTER TABLE raw_order_created
    ALTER COLUMN contract_name SET NOT NULL,
    ALTER COLUMN contract_version SET NOT NULL,
    ALTER COLUMN original_payload SET NOT NULL,
    ADD CONSTRAINT ck_raw_order_created_contract_version
        CHECK (contract_version > 0);


ALTER TABLE raw_order_paid
    ADD COLUMN IF NOT EXISTS contract_name varchar,
    ADD COLUMN IF NOT EXISTS contract_version integer,
    ADD COLUMN IF NOT EXISTS original_payload jsonb;


UPDATE raw_order_paid
SET
    contract_name = 'order_paid',
    contract_version = COALESCE(
        substring(
            kafka_topic
            FROM '\.v([0-9]+)$'
        )::integer,
        1
    ),
    original_payload = jsonb_build_object(
        'order_id', order_id,
        'payment_id', payment_id,
        'paid_amount', paid_amount,
        'currency', currency,
        'paid_at', paid_at
    )
WHERE
    contract_name IS NULL
    OR contract_version IS NULL
    OR original_payload IS NULL;


ALTER TABLE raw_order_paid
    ALTER COLUMN contract_name SET NOT NULL,
    ALTER COLUMN contract_version SET NOT NULL,
    ALTER COLUMN original_payload SET NOT NULL,
    ADD CONSTRAINT ck_raw_order_paid_contract_version
        CHECK (contract_version > 0);


ALTER TABLE raw_order_cancelled
    ADD COLUMN IF NOT EXISTS contract_name varchar,
    ADD COLUMN IF NOT EXISTS contract_version integer,
    ADD COLUMN IF NOT EXISTS original_payload jsonb;


UPDATE raw_order_cancelled
SET
    contract_name = 'order_cancelled',
    contract_version = COALESCE(
        substring(
            kafka_topic
            FROM '\.v([0-9]+)$'
        )::integer,
        1
    ),
    original_payload = jsonb_build_object(
        'order_id', order_id,
        'cancellation_id', cancellation_id,
        'cancellation_reason', cancellation_reason,
        'cancelled_at', cancelled_at
    )
WHERE
    contract_name IS NULL
    OR contract_version IS NULL
    OR original_payload IS NULL;


ALTER TABLE raw_order_cancelled
    ALTER COLUMN contract_name SET NOT NULL,
    ALTER COLUMN contract_version SET NOT NULL,
    ALTER COLUMN original_payload SET NOT NULL,
    ADD CONSTRAINT ck_raw_order_cancelled_contract_version
        CHECK (contract_version > 0);


ALTER TABLE dead_letter_events
    ADD COLUMN IF NOT EXISTS contract_name varchar,
    ADD COLUMN IF NOT EXISTS contract_version integer,
    ADD COLUMN IF NOT EXISTS raw_payload bytea;


UPDATE dead_letter_events
SET
    contract_name = COALESCE(
        substring(
            kafka_topic
            FROM '([^.]+)\.v[0-9]+$'
        ),
        'unknown'
    ),
    contract_version = COALESCE(
        substring(
            kafka_topic
            FROM '\.v([0-9]+)$'
        )::integer,
        1
    ),
    raw_payload = CASE
        WHEN event_payload IS NULL THEN NULL
        ELSE convert_to(event_payload::text, 'UTF8')
    END
WHERE
    contract_name IS NULL
    OR contract_version IS NULL
    OR raw_payload IS NULL;


ALTER TABLE dead_letter_events
    ALTER COLUMN contract_name SET NOT NULL,
    ALTER COLUMN contract_version SET NOT NULL,
    ADD CONSTRAINT ck_dead_letter_contract_version
        CHECK (contract_version > 0);
