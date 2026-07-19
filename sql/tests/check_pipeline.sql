DO $$
DECLARE
    actual_count bigint;
BEGIN
    SELECT COUNT(*)
    INTO actual_count
    FROM raw_order_created;

    IF actual_count <> 3 THEN
        RAISE EXCEPTION
            'raw_order_created: expected 3 rows, got %',
            actual_count;
    END IF;

    SELECT COUNT(*)
    INTO actual_count
    FROM raw_order_paid;

    IF actual_count <> 2 THEN
        RAISE EXCEPTION
            'raw_order_paid: expected 2 rows, got %',
            actual_count;
    END IF;

    SELECT COUNT(*)
    INTO actual_count
    FROM raw_order_cancelled;

    IF actual_count <> 1 THEN
        RAISE EXCEPTION
            'raw_order_cancelled: expected 1 row, got %',
            actual_count;
    END IF;

    SELECT COUNT(*)
    INTO actual_count
    FROM dead_letter_events;

    IF actual_count <> 1 THEN
        RAISE EXCEPTION
            'dead_letter_events: expected 1 row, got %',
            actual_count;
    END IF;

    SELECT COUNT(*)
    INTO actual_count
    FROM dds_orders;

    IF actual_count <> 2 THEN
        RAISE EXCEPTION
            'dds_orders: expected 2 rows, got %',
            actual_count;
    END IF;

    SELECT COUNT(*)
    INTO actual_count
    FROM mart_daily_orders;

    IF actual_count <> 1 THEN
        RAISE EXCEPTION
            'mart_daily_orders: expected 1 row, got %',
            actual_count;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM dds_orders
        WHERE order_id = 'ord_1001'
          AND status = 'paid'
          AND payments_cnt = 2
          AND dq_multiple_payments_flg = TRUE
    ) THEN
        RAISE EXCEPTION
            'ord_1001 does not have expected DDS state';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM dds_orders
        WHERE order_id = 'ord_1002'
          AND status = 'cancelled'
          AND cancellations_cnt = 1
    ) THEN
        RAISE EXCEPTION
            'ord_1002 does not have expected DDS state';
    END IF;

    RAISE NOTICE 'Integration checks passed';
END
$$;