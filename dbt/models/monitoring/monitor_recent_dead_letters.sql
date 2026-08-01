{{ config(materialized='view') }}

select
    id,
    load_dttm,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    contract_name,
    contract_version,
    error_message

from {{ source('operations', 'dead_letter_events') }}
