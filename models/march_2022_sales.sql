{{
    config(
        materialized='view'
    )
}}

with march_orders as (
    select *
    from {{ ref('orders') }}
    where order_date >= '2022-03-01' and order_date <= '2022-03-31'
),

total_sales as (
    select sum(amount) as total_sales
    from march_orders
)

select * from total_sales
