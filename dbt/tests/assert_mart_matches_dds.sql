with expected as (
    select
        orders.created_at::date as order_dt,
        count(*)::integer as created_orders_cnt,
        count(*) filter (
            where orders.status = 'paid'
        )::integer as paid_orders_cnt,
        count(*) filter (
            where orders.status = 'cancelled'
        )::integer as cancelled_orders_cnt,
        count(*) filter (
            where orders.status = 'created'
        )::integer as created_status_orders_cnt,
        sum(orders.order_amount)::numeric(14, 2)
            as gross_order_amount,
        coalesce(
            sum(orders.paid_amount) filter (
                where orders.status = 'paid'
            ),
            0
        )::numeric(14, 2) as paid_revenue,
        count(*) filter (
            where
                orders.duplicate_created_events_cnt > 0
                or orders.dq_multiple_payments_flg
                or orders.dq_multiple_cancellations_flg
                or orders.dq_payment_amount_mismatch_flg
                or orders.dq_payment_currency_mismatch_flg
                or orders.dq_payment_before_creation_flg
                or orders.dq_cancellation_before_creation_flg
                or orders.dq_payment_after_cancellation_flg
        )::integer as orders_with_dq_cnt
    from {{ ref('dds_orders') }} as orders
    group by orders.created_at::date
),

actual as (
    select
        mart.order_dt,
        mart.created_orders_cnt,
        mart.paid_orders_cnt,
        mart.cancelled_orders_cnt,
        mart.created_status_orders_cnt,
        mart.gross_order_amount,
        mart.paid_revenue,
        mart.orders_with_dq_cnt
    from {{ ref('mart_daily_orders') }} as mart
),

differences as (
    (
        select *
        from expected
        except
        select *
        from actual
    )

    union all

    (
        select *
        from actual
        except
        select *
        from expected
    )
)

select *
from differences
