select
    status
from {{ ref('completed_order') }}
where status != 'completed'
