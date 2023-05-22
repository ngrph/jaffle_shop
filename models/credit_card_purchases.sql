{{
    config(materialized='table')
}}

select *
from {{ ref('stg_payments') }}
where payment_method = 'credit_card'