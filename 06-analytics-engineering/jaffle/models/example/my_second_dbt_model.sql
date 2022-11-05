
-- Use the `ref` function to select from other models
-- jinja template : python 

select *
from {{ ref('my_first_dbt_model') }}
where id = 1
