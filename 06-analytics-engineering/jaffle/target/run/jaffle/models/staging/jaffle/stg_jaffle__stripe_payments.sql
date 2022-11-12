
  create view "postgres"."public"."stg_jaffle__stripe_payments__dbt_tmp" as (
    with

source as (

    select * from "postgres"."public"."stripe_payments"

)

, final as (

    select
        id
        , order_id
        , payment_method
        , amount
        , status
        , created

    from source

)

select * from final
  );