{{ config(materialized='view') }}

with latest_run as (
    select *
    from {{ source('operations', 'analytics_run_history') }}
    order by started_at desc
    limit 1
),

latest_completed_run as (
    select *
    from {{ source('operations', 'analytics_run_history') }}
    where status <> 'running'
    order by started_at desc
    limit 1
),

last_successful_build as (
    select max(finished_at) as finished_at
    from {{ source('operations', 'analytics_run_history') }}
    where
        build_status = 'success'
        and status in ('success', 'warning')
),

run_window as (
    select
        count(*) filter (
            where
                started_at >= current_timestamp - interval '24 hours'
                and status <> 'running'
        )::bigint as run_count_24h,
        count(*) filter (
            where
                started_at >= current_timestamp - interval '24 hours'
                and status <> 'running'
                and build_status = 'success'
        )::bigint as successful_build_count_24h,
        count(*) filter (
            where status = 'running'
        )::bigint as active_run_count
    from {{ source('operations', 'analytics_run_history') }}
),

source_freshness as (
    select max(kafka_load_dttm) as latest_raw_created_at
    from {{ source('raw_events', 'raw_order_created') }}
),

dead_letters as (
    select count(*)::bigint as dead_letter_count_24h
    from {{ source('operations', 'dead_letter_events') }}
    where load_dttm >= current_timestamp - interval '24 hours'
),

data_quality as (
    select count(*) filter (
        where
            duplicate_created_events_cnt > 0
            or dq_multiple_payments_flg
            or dq_multiple_cancellations_flg
            or dq_payment_amount_mismatch_flg
            or dq_payment_currency_mismatch_flg
            or dq_payment_before_creation_flg
            or dq_cancellation_before_creation_flg
            or dq_payment_after_cancellation_flg
    )::bigint as dq_affected_orders
    from {{ ref('dds_orders') }}
),

orphans as (
    select count(*)::bigint as orphan_order_count
    from {{ ref('dq_orphan_order_events') }}
),

metrics as (
    select
        current_timestamp as observed_at,
        latest_run.status as latest_run_status,
        latest_run.trigger_type as latest_run_trigger,
        latest_run.started_at as latest_run_started_at,
        latest_completed_run.status as latest_completed_status,
        latest_completed_run.freshness_status,
        latest_completed_run.build_status,
        latest_completed_run.duration_seconds as latest_duration_seconds,
        latest_completed_run.error_message as latest_error_message,
        last_successful_build.finished_at as last_successful_build_at,
        source_freshness.latest_raw_created_at,
        run_window.run_count_24h,
        run_window.successful_build_count_24h,
        run_window.active_run_count,
        case
            when run_window.run_count_24h = 0 then 0::numeric
            else
                run_window.successful_build_count_24h::numeric
                / run_window.run_count_24h
        end as build_success_rate_24h,
        greatest(
            extract(
                epoch from (
                    current_timestamp
                    - last_successful_build.finished_at
                )
            ),
            0
        ) as last_success_age_seconds,
        greatest(
            extract(
                epoch from (
                    current_timestamp
                    - source_freshness.latest_raw_created_at
                )
            ),
            0
        ) as source_freshness_age_seconds,
        dead_letters.dead_letter_count_24h,
        data_quality.dq_affected_orders,
        orphans.orphan_order_count
    from run_window
    cross join source_freshness
    cross join dead_letters
    cross join data_quality
    cross join orphans
    left join latest_run on true
    left join latest_completed_run on true
    cross join last_successful_build
),

classified as (
    select
        *,
        case
            when latest_completed_status is null then 2
            when latest_completed_status = 'error' then 2
            when build_status = 'error' then 2
            when last_success_age_seconds is null then 2
            when last_success_age_seconds > 900 then 2
            when latest_raw_created_at is null then 2
            when source_freshness_age_seconds > 3600 then 2
            when latest_completed_status = 'warning' then 1
            when freshness_status in ('warn', 'error') then 1
            when source_freshness_age_seconds > 900 then 1
            when dead_letter_count_24h > 0 then 1
            when dq_affected_orders > 0 then 1
            when orphan_order_count > 0 then 1
            else 0
        end as health_code
    from metrics
)

select
    *,
    case health_code
        when 0 then 'healthy'
        when 1 then 'warning'
        else 'critical'
    end as overall_health,
    case
        when latest_completed_status is null
            then 'No completed analytics run'
        when latest_completed_status = 'error' or build_status = 'error'
            then 'Latest dbt build failed'
        when last_success_age_seconds is null
            then 'No successful dbt build'
        when last_success_age_seconds > 900
            then 'No successful dbt build within 15 minutes'
        when latest_raw_created_at is null
            then 'No order_created events in RAW'
        when source_freshness_age_seconds > 3600
            then 'RAW order_created is more than 60 minutes stale'
        when latest_completed_status = 'warning'
            then 'Latest analytics run completed with a warning'
        when freshness_status in ('warn', 'error')
            then 'Latest dbt source freshness check did not pass'
        when source_freshness_age_seconds > 900
            then 'RAW order_created is more than 15 minutes stale'
        when dead_letter_count_24h > 0
            then 'Dead-letter events were recorded in the last 24 hours'
        when dq_affected_orders > 0
            then 'Current DDS orders contain data-quality issues'
        when orphan_order_count > 0
            then 'Orphan payment or cancellation events are present'
        else 'Pipeline is healthy'
    end as health_reason

from classified
