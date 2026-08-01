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
    cancellation_id,
    cancellation_reason,
    cancelled_at

from {{ source('raw_events', 'raw_order_cancelled') }}
