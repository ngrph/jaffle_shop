{% set march_start = '2018-03-01' %}
{% set march_end = '2018-03-31' %}

with march_orders as (
    select *
    from {{ ref('orders') }}
    where order_date >= '{{ march_start }}' and order_date <= '{{ march_end }}'
),

total_sales as (
    select sum(amount) as total_sales
    from march_orders
)

select * from total_sales
