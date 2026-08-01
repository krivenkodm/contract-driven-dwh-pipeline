{{ config(tags=['parity']) }}

with legacy as (
    select
        order_dt,
        created_orders_cnt,
        paid_orders_cnt,
        cancelled_orders_cnt,
        created_status_orders_cnt,
        gross_order_amount,
        paid_revenue,
        orders_with_dq_cnt
    from {{ source('legacy_analytics', 'mart_daily_orders') }}
),

actual as (
    select
        order_dt,
        created_orders_cnt,
        paid_orders_cnt,
        cancelled_orders_cnt,
        created_status_orders_cnt,
        gross_order_amount,
        paid_revenue,
        orders_with_dq_cnt
    from {{ ref('mart_daily_orders') }}
),

differences as (
    (
        select *
        from legacy
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
        from legacy
    )
)

select *
from differences
