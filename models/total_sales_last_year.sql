{% set current_date = 'current_date()' %}

with orders_last_year as (
    select *
    from {{ ref('orders') }}
    where order_date >= dateadd(year, -1, {{ current_date }})
),

total_sales as (
    select sum(amount) as total_sales_last_year
    from orders_last_year
)

select * from total_sales