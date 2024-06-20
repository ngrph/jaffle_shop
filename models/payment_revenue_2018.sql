{{
    config(
        materialized='table'
    )
}}

with payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

joined_data as (
    select
        p.payment_method,
        p.amount,
        o.order_date as payment_date
    from payments p
    join orders o on p.order_id = o.order_id
),

filtered_payments as (
    select
        payment_method,
        sum(amount) as revenue
    from joined_data
    where extract(year from payment_date) = 2018
    group by payment_method
)

select * from filtered_payments