select
    order_status
from "postgres"."public"."completed_orders"
where order_status != 'completed'