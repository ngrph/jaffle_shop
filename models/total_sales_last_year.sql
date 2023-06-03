{% set last_year_start = '2021-01-01' %}
{% set last_year_end = '2021-12-31' %}

with orders as (
    select * from {{ ref('orders') }}
    where order_date between '{{ last_year_start }}' and '{{ last_year_end }}'
),

total_sales as (
    select sum(amount) as total_sales_last_year from orders
)

select * from total_sales