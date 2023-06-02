{% set start_date = '2018-03-01' %}
{% set end_date = '2018-03-31' %}

with march_orders as (
    select * from {{ ref('orders') }}
    where order_date >= '{{ start_date }}'
    and order_date <= '{{ end_date }}'
),

total_sales as (
    select sum(amount) as total_amount
    from march_orders
)

select * from total_sales