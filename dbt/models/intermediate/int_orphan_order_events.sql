with orphan_events as (
    select
        payments.order_id,
        'payment'::varchar as event_type,
        payments.paid_at as event_at

    from {{ ref('stg_order_paid') }} as payments

    union all

    select
        cancellations.order_id,
        'cancellation'::varchar as event_type,
        cancellations.cancelled_at as event_at

    from {{ ref('stg_order_cancelled') }} as cancellations
),

created_order_ids as (
    select distinct
        created.order_id

    from {{ ref('stg_order_created') }} as created
)

select
    orphan_events.order_id,

    count(*) filter (
        where orphan_events.event_type = 'payment'
    )::integer as payments_cnt,

    count(*) filter (
        where orphan_events.event_type = 'cancellation'
    )::integer as cancellations_cnt,

    min(orphan_events.event_at) as first_event_at,
    max(orphan_events.event_at) as last_event_at

from orphan_events

left join created_order_ids
    on created_order_ids.order_id = orphan_events.order_id

where created_order_ids.order_id is null

group by
    orphan_events.order_id
