with created_source as (
    select *
    from {{ ref('stg_order_created') }} as created_events
),

created_ranked as (
    select
        created_source.*,

        row_number() over (
            partition by order_id
            order by
                created_at,
                kafka_load_dttm,
                kafka_partition,
                kafka_offset,
                raw_id
        ) as event_rank,

        count(*) over (
            partition by order_id
        ) as created_events_cnt,

        max(raw_id) over (
            partition by order_id
        ) as max_created_raw_id

    from created_source
),

created as (
    select *
    from created_ranked
    where event_rank = 1
),

payment_source as (
    select *
    from {{ ref('stg_order_paid') }} as payment_events
),

payments_ranked as (
    select
        payment_source.*,

        row_number() over (
            partition by payment_source.order_id
            order by
                payment_source.paid_at desc,
                payment_source.kafka_load_dttm desc,
                payment_source.kafka_partition desc,
                payment_source.kafka_offset desc,
                payment_source.raw_id desc
        ) as event_rank,

        count(*) over (
            partition by payment_source.order_id
        ) as payments_cnt,

        max(payment_source.raw_id) over (
            partition by payment_source.order_id
        ) as max_paid_raw_id,

        bool_or(
            payment_source.paid_amount <> created.amount
        ) over (
            partition by payment_source.order_id
        ) as dq_payment_amount_mismatch_flg,

        bool_or(
            payment_source.currency <> created.currency
        ) over (
            partition by payment_source.order_id
        ) as dq_payment_currency_mismatch_flg,

        bool_or(
            payment_source.paid_at < created.created_at
        ) over (
            partition by payment_source.order_id
        ) as dq_payment_before_creation_flg

    from payment_source

    inner join created
        on created.order_id = payment_source.order_id
),

payments as (
    select *
    from payments_ranked
    where event_rank = 1
),

cancellation_source as (
    select *
    from {{ ref('stg_order_cancelled') }} as cancellation_events
),

cancellations_ranked as (
    select
        cancellation_source.*,

        row_number() over (
            partition by cancellation_source.order_id
            order by
                cancellation_source.cancelled_at desc,
                cancellation_source.kafka_load_dttm desc,
                cancellation_source.kafka_partition desc,
                cancellation_source.kafka_offset desc,
                cancellation_source.raw_id desc
        ) as event_rank,

        count(*) over (
            partition by cancellation_source.order_id
        ) as cancellations_cnt,

        max(cancellation_source.raw_id) over (
            partition by cancellation_source.order_id
        ) as max_cancelled_raw_id,

        min(cancellation_source.cancelled_at) over (
            partition by cancellation_source.order_id
        ) as first_cancelled_at,

        bool_or(
            cancellation_source.cancelled_at < created.created_at
        ) over (
            partition by cancellation_source.order_id
        ) as dq_cancellation_before_creation_flg

    from cancellation_source

    inner join created
        on created.order_id = cancellation_source.order_id
),

cancellations as (
    select *
    from cancellations_ranked
    where event_rank = 1
)

select
    created.order_id,
    created.customer_id,
    created.source_channel,
    created.amount as order_amount,
    created.currency,
    created.created_at,

    case
        when cancellations.cancelled_at is not null
            then 'cancelled'
        when payments.paid_at is not null
            then 'paid'
        else 'created'
    end as status,

    payments.payment_id,
    payments.paid_amount,
    payments.currency as payment_currency,
    payments.paid_at,

    cancellations.cancellation_id,
    cancellations.cancellation_reason,
    cancellations.cancelled_at,

    coalesce(
        payments.payments_cnt,
        0
    )::integer as payments_cnt,

    coalesce(
        cancellations.cancellations_cnt,
        0
    )::integer as cancellations_cnt,

    greatest(
        created.created_events_cnt - 1,
        0
    )::integer as duplicate_created_events_cnt,

    coalesce(
        payments.payments_cnt,
        0
    ) > 1 as dq_multiple_payments_flg,

    coalesce(
        cancellations.cancellations_cnt,
        0
    ) > 1 as dq_multiple_cancellations_flg,

    coalesce(
        payments.dq_payment_amount_mismatch_flg,
        false
    ) as dq_payment_amount_mismatch_flg,

    coalesce(
        payments.dq_payment_currency_mismatch_flg,
        false
    ) as dq_payment_currency_mismatch_flg,

    coalesce(
        payments.dq_payment_before_creation_flg,
        false
    ) as dq_payment_before_creation_flg,

    coalesce(
        cancellations.dq_cancellation_before_creation_flg,
        false
    ) as dq_cancellation_before_creation_flg,

    (
        payments.paid_at is not null
        and cancellations.first_cancelled_at is not null
        and payments.paid_at > cancellations.first_cancelled_at
    ) as dq_payment_after_cancellation_flg,

    created.max_created_raw_id,

    coalesce(
        payments.max_paid_raw_id,
        0
    )::bigint as max_paid_raw_id,

    coalesce(
        cancellations.max_cancelled_raw_id,
        0
    )::bigint as max_cancelled_raw_id

from created

left join payments
    on payments.order_id = created.order_id

left join cancellations
    on cancellations.order_id = created.order_id
