{{
  config(
    materialized='view'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
)

select
    orders.*,
    payments.payment_id,
    payments.payment_method,
    payments.amount
from orders
join payments on orders.order_id = payments.order_id