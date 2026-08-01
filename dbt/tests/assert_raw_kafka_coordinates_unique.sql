with kafka_messages as (
    select
        kafka_topic,
        kafka_partition,
        kafka_offset,
        'order_created' as event_name
    from {{ ref('stg_order_created') }}

    union all

    select
        kafka_topic,
        kafka_partition,
        kafka_offset,
        'order_paid' as event_name
    from {{ ref('stg_order_paid') }}

    union all

    select
        kafka_topic,
        kafka_partition,
        kafka_offset,
        'order_cancelled' as event_name
    from {{ ref('stg_order_cancelled') }}
)

select
    event_name,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    count(*) as messages_cnt
from kafka_messages
group by
    event_name,
    kafka_topic,
    kafka_partition,
    kafka_offset
having count(*) > 1
