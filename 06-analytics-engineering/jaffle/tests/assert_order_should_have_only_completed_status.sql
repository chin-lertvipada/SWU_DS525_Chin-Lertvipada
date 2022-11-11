select status 
from {{ ref('order') }}
where status != 'completed'