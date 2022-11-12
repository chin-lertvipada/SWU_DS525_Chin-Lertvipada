
    
    

select
    order_id as unique_field,
    count(*) as n_records

from "postgres"."public"."completed_orders"
where order_id is not null
group by order_id
having count(*) > 1


