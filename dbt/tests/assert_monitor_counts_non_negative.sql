select
    'pipeline_health' as model_name

from {{ ref('monitor_pipeline_health') }}

where
    run_count_24h < 0
    or successful_build_count_24h < 0
    or active_run_count < 0
    or dead_letter_count_24h < 0
    or dq_affected_orders < 0
    or orphan_order_count < 0

union all

select
    'raw_event_volume' as model_name

from {{ ref('monitor_raw_event_volume_hourly') }}

where events_count < 0

union all

select
    'dead_letter_volume' as model_name

from {{ ref('monitor_dead_letter_volume_hourly') }}

where dead_letter_count < 0

union all

select
    'data_quality_issues' as model_name

from {{ ref('monitor_data_quality_issues') }}

where affected_orders < 0
