{{
    config(
        materialized='table'
    )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_date
    from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        payment_method,
        amount
    from {{ ref('stg_payments') }}
)

select
    o.customer_id,
    o.order_date,
    p.payment_method,
    p.amount
from orders o
join payments p
    on o.order_id = p.order_id