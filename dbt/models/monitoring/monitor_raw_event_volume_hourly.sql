{{ config(materialized='view') }}

with raw_events as (
    select
        kafka_load_dttm,
        'order_created'::text as event_type
    from {{ source('raw_events', 'raw_order_created') }}

    union all

    select
        kafka_load_dttm,
        'order_paid'::text as event_type
    from {{ source('raw_events', 'raw_order_paid') }}

    union all

    select
        kafka_load_dttm,
        'order_cancelled'::text as event_type
    from {{ source('raw_events', 'raw_order_cancelled') }}
)

select
    date_trunc('hour', kafka_load_dttm) as event_hour,
    event_type,
    count(*)::bigint as events_count

from raw_events

group by
    date_trunc('hour', kafka_load_dttm),
    event_type
