{{ config(tags=['parity']) }}

with legacy as (
    select
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
        dq_payment_after_cancellation_flg
    from {{ source('legacy_analytics', 'dds_orders') }}
),

actual as (
    select
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
        dq_payment_after_cancellation_flg
    from {{ ref('dds_orders') }}
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
