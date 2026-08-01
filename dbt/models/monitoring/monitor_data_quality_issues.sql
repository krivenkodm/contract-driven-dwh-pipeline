{{ config(materialized='view') }}

select
    1 as sort_order,
    'duplicate_created_events'::text as issue_type,
    'Duplicate creation events'::text as issue_label,
    count(*) filter (
        where duplicate_created_events_cnt > 0
    )::bigint as affected_orders
from {{ ref('dds_orders') }}

union all

select
    2,
    'multiple_payments',
    'Multiple payments',
    count(*) filter (where dq_multiple_payments_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    3,
    'multiple_cancellations',
    'Multiple cancellations',
    count(*) filter (where dq_multiple_cancellations_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    4,
    'payment_amount_mismatch',
    'Payment amount mismatch',
    count(*) filter (where dq_payment_amount_mismatch_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    5,
    'payment_currency_mismatch',
    'Payment currency mismatch',
    count(*) filter (where dq_payment_currency_mismatch_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    6,
    'payment_before_creation',
    'Payment before creation',
    count(*) filter (where dq_payment_before_creation_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    7,
    'cancellation_before_creation',
    'Cancellation before creation',
    count(*) filter (where dq_cancellation_before_creation_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    8,
    'payment_after_cancellation',
    'Payment after cancellation',
    count(*) filter (where dq_payment_after_cancellation_flg)::bigint
from {{ ref('dds_orders') }}

union all

select
    9,
    'orphan_lifecycle_events',
    'Orphan lifecycle events',
    count(*)::bigint
from {{ ref('dq_orphan_order_events') }}
