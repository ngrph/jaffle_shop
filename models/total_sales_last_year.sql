{{
    config(
        materialized='table'
    )
}}

with last_year_sales as (
    select
        sum(amount) as total_sales_last_year
    from {{ ref('orders') }}
    where
        order_date >= date_trunc('year', current_date - interval '1 year')
        and order_date < date_trunc('year', current_date)
)

select * from last_year_sales
