select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      select
    order_status
from "postgres"."public"."completed_orders"
where order_status != 'completed'
      
    ) dbt_internal_test