with

source as (

    select * from "postgres"."public"."jaffle_shop_orders"

)

, final as (

    select
        id
        , user_id
        , order_date
        , status

    from source

)

select * from final