{{ config(materialized='view') }}

select
    date_trunc('hour', load_dttm) as event_hour,
    count(*)::bigint as dead_letter_count

from {{ source('operations', 'dead_letter_events') }}

group by
    date_trunc('hour', load_dttm)
