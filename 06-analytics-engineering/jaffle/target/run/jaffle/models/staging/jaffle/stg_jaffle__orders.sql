
  create view "postgres"."public"."stg_jaffle__orders__dbt_tmp" as (
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
  );