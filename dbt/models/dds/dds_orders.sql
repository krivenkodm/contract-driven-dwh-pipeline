-- depends_on: {{ ref('stg_order_created') }}
-- depends_on: {{ ref('stg_order_paid') }}
-- depends_on: {{ ref('stg_order_cancelled') }}

{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        indexes=[
            {'columns': ['order_id'], 'unique': true},
            {'columns': ['processed_dttm']}
        ]
    )
}}

with enriched_orders as (
    select *
    from {{ ref('int_orders_enriched') }} as enriched
),

{% if is_incremental() %}

source_watermarks as (
    select
        coalesce(max(max_created_raw_id), 0)::bigint
            as max_created_raw_id,
        coalesce(max(max_paid_raw_id), 0)::bigint
            as max_paid_raw_id,
        coalesce(max(max_cancelled_raw_id), 0)::bigint
            as max_cancelled_raw_id

    from {{ this }}
),

affected_order_ids as (
    select created.order_id
    from {{ ref('stg_order_created') }} as created
    cross join source_watermarks
    where created.raw_id > source_watermarks.max_created_raw_id

    union

    select paid.order_id
    from {{ ref('stg_order_paid') }} as paid
    cross join source_watermarks
    where paid.raw_id > source_watermarks.max_paid_raw_id

    union

    select cancelled.order_id
    from {{ ref('stg_order_cancelled') }} as cancelled
    cross join source_watermarks
    where cancelled.raw_id > source_watermarks.max_cancelled_raw_id
),

{% else %}

affected_order_ids as (
    select enriched_orders.order_id
    from enriched_orders
),

{% endif %}

prepared as (
    select
        enriched_orders.*,
        current_timestamp as processed_dttm

    from enriched_orders

    inner join affected_order_ids
        on affected_order_ids.order_id = enriched_orders.order_id
)

select *
from prepared
