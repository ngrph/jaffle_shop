{{
    config(materialized='view')
}}

with total_revenue as (
    select sum(amount) as total_revenue
    from {{ ref('credit_card_payments') }}
    where payment_method = 'Credit Card'
)

select * from total_revenue
