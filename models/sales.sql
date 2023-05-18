{{
  config(
    materialized='view'
  )
}}

with orders as (
    select
        order_id,
        customer_id,
        order_date,
        status
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
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    p.payment_method,
    p.amount
from orders o
join payments p
on o.order_id = p.order_id