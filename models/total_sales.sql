{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (
    select * from {{ ref('orders') }}
),

total_sales as (
    select sum(amount) as total_sales from orders
)

select * from total_sales