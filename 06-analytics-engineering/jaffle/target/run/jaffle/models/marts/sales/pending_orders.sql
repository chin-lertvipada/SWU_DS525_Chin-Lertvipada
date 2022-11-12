
  create view "postgres"."public"."pending_orders__dbt_tmp" as (
    with

int_orders_customers_joined as (

    select * from "postgres"."public"."int_orders_customers_joined"

)

, final as (

    select
        order_id
        , order_date
        , order_status
        , customer_name

    from int_orders_customers_joined
    where order_status = 'pending'

)

select * from final
  );