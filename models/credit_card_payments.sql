{{
    config(materialized='view')
}}

with credit_card_payments as (
    select *
    from {{ ref('stg_payments') }}
    where payment_method = 'Credit Card'
)

select * from credit_card_payments