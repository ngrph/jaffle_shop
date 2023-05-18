{{
  config(
    materialized='table'
  )
}}

with orders as (
  select * from {{ ref('stg_orders') }}
),

payments as (
  select * from {{ ref('stg_payments') }}
),

joined_data as (
  select
    o.customer_id,
    sum(p.amount) as total_spent
  from orders o
  join payments p on o.order_id = p.order_id
  group by o.customer_id
)

select *
from joined_data
order by total_spent desc
limit 3