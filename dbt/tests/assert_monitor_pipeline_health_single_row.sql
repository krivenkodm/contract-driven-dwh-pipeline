select count(*) as health_rows

from {{ ref('monitor_pipeline_health') }}

having count(*) <> 1
