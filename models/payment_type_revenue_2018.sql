{{
    config(materialized='view')
}}

with payments_2018 as (
    select p.payment_method, p.amount, o.order_date
    from {{ ref('credit_card_payments') }} as p
    join {{ ref('stg_orders') }} as o
    on p.order_id = o.order_id
    where date_part('year', o.order_date) = 2018
)

select payment_method as payment_type, sum(amount) as total_revenue
from payments_2018
group by payment_method