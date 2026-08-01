select
    raw_id,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    kafka_load_dttm,
    contract_name,
    contract_version,
    original_payload,
    order_id,
    customer_id,
    amount,
    currency,
    created_at,
    source_channel

from {{ source('raw_events', 'raw_order_created') }}
