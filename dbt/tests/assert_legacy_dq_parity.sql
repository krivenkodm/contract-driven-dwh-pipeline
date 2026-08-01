{{ config(tags=['parity']) }}

with legacy as (
    select
        order_id,
        payments_cnt,
        cancellations_cnt,
        first_event_at,
        last_event_at
    from {{ source('legacy_analytics', 'dq_orphan_order_events') }}
),

actual as (
    select
        order_id,
        payments_cnt,
        cancellations_cnt,
        first_event_at,
        last_event_at
    from {{ ref('dq_orphan_order_events') }}
),

differences as (
    (
        select *
        from legacy
        except
        select *
        from actual
    )

    union all

    (
        select *
        from actual
        except
        select *
        from legacy
    )
)

select *
from differences
