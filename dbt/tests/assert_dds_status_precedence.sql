select *
from {{ ref('dds_orders') }} as orders
where
    (
        orders.cancellation_id is not null
        and orders.status <> 'cancelled'
    )
    or (
        orders.cancellation_id is null
        and orders.payment_id is not null
        and orders.status <> 'paid'
    )
    or (
        orders.cancellation_id is null
        and orders.payment_id is null
        and orders.status <> 'created'
    )
