{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['order_id'], 'unique': true}
        ]
    )
}}

select
    orphan_events.order_id,
    orphan_events.payments_cnt,
    orphan_events.cancellations_cnt,
    orphan_events.first_event_at,
    orphan_events.last_event_at,
    current_timestamp as processed_dttm

from {{ ref('int_orphan_order_events') }} as orphan_events
