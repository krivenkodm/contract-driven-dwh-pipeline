{{ config(materialized='view') }}

select
    run_id,
    trigger_type,
    status,
    started_at,
    finished_at,
    duration_seconds,
    freshness_status,
    build_status,
    dbt_invocation_id,
    total_nodes,
    successful_nodes,
    warned_nodes,
    failed_nodes,
    case
        when build_status = 'success' then 1
        else 0
    end as build_succeeded,
    error_message

from {{ source('operations', 'analytics_run_history') }}
