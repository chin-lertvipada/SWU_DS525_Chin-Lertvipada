with

source as (

    select * from "postgres"."public"."jaffle_shop_customers"

)

, final as (

    select
        id
        , first_name || ' ' || last_name as name

    from source

)

select * from final