-- depends_on: {{ ref('dds_orders_history') }}

{{
    config(
        materialized='incremental',
        unique_key='order_dt',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        post_hook=(
            "delete from {{ this }} where created_orders_cnt = 0"
        ),
        indexes=[
            {'columns': ['order_dt'], 'unique': true}
        ]
    )
}}

with affected_dates as (
    {% if is_incremental() %}

    select distinct
        history.created_at::date as order_dt

    from {{ ref('dds_orders_history') }} as history

    where
        history.dbt_valid_from > coalesce(
            (
                select max(existing.processed_dttm)
                from {{ this }} as existing
            ),
            '1900-01-01T00:00:00Z'::timestamptz
        )

        or history.dbt_valid_to > coalesce(
            (
                select max(existing.processed_dttm)
                from {{ this }} as existing
            ),
            '1900-01-01T00:00:00Z'::timestamptz
        )

    {% else %}

    select distinct
        orders.created_at::date as order_dt

    from {{ ref('dds_orders') }} as orders

    {% endif %}
),

aggregated_dates as (
    select
        affected_dates.order_dt,

        count(orders.order_id)::integer
            as created_orders_cnt,

        count(orders.order_id) filter (
            where orders.status = 'paid'
        )::integer as paid_orders_cnt,

        count(orders.order_id) filter (
            where orders.status = 'cancelled'
        )::integer as cancelled_orders_cnt,

        count(orders.order_id) filter (
            where orders.status = 'created'
        )::integer as created_status_orders_cnt,

        coalesce(
            sum(orders.order_amount),
            0
        )::numeric(14, 2) as gross_order_amount,

        coalesce(
            sum(orders.paid_amount) filter (
                where orders.status = 'paid'
            ),
            0
        )::numeric(14, 2) as paid_revenue,

        count(orders.order_id) filter (
            where
                orders.duplicate_created_events_cnt > 0
                or orders.dq_multiple_payments_flg
                or orders.dq_multiple_cancellations_flg
                or orders.dq_payment_amount_mismatch_flg
                or orders.dq_payment_currency_mismatch_flg
                or orders.dq_payment_before_creation_flg
                or orders.dq_cancellation_before_creation_flg
                or orders.dq_payment_after_cancellation_flg
        )::integer as orders_with_dq_cnt,

        current_timestamp as processed_dttm

    from affected_dates

    left join {{ ref('dds_orders') }} as orders
        on orders.created_at::date = affected_dates.order_dt

    group by
        affected_dates.order_dt
)

select *
from aggregated_dates
