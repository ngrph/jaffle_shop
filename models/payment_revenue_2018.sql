{{
  config(materialized='view')
}}

with orders_filtered as (
  select *
  from {{ ref('orders') }}
  where date_part('year', order_date) = 2018
),

joined_data as (
  select
    o.order_id,
    o.order_date,
    o.customer_id,
    o.status,
    cc.payment_method,
    cc.amount
  from orders_filtered as o
  join {{ ref('credit_card_payments') }} as cc
    on o.order_id = cc.order_id
),

grouped_data as (
  select
    payment_method,
    sum(amount) as revenue
  from joined_data
  group by payment_method
)

select *
from grouped_data
order by revenue desc
