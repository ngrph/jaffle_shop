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
),

sales as (
  select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    p.payment_method,
    p.amount
  from orders o
  join payments p on o.order_id = p.order_id
)

select * from sales