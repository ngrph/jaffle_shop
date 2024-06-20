{{
  config(
    materialized='view'
  )
}}

with payments as (
    select * from {{ ref('stg_payments') }}
),

revenue_by_payment_method as (
    select
        payment_method,
        sum(amount) as total_revenue
    from payments
    group by payment_method
)

select * from revenue_by_payment_method order by total_revenue desc