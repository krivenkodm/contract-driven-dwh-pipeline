{% snapshot dds_orders_history %}

{{
    config(
        target_schema='dbt',
        unique_key='order_id',
        strategy='timestamp',
        updated_at='snapshot_updated_at',
        invalidate_hard_deletes=true
    )
}}

select
    orders.*,
    orders.processed_dttm at time zone 'UTC'
        as snapshot_updated_at
from {{ ref('dds_orders') }} as orders

{% endsnapshot %}
