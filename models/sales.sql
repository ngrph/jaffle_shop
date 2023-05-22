{{
    config(materialized='table')
}}

with sales as (
    select
        order_id,
        payment_method,
        amount
    from {{ ref('stg_payments') }}
)

select * from sales