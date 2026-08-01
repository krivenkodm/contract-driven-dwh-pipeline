select orphan_events.*
from {{ ref('dq_orphan_order_events') }} as orphan_events
inner join {{ ref('stg_order_created') }} as created
    on created.order_id = orphan_events.order_id
