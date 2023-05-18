
with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
joined_data as (
    select
        c.CUSTOMER_ID,
        o.ORDER_ID,
        p.AMOUNT
    from customers c
    join orders o on c.CUSTOMER_ID = o.CUSTOMER_ID
    join payments p on o.ORDER_ID = p.ORDER_ID
),
customer_spent_amount as (
    select
        CUSTOMER_ID,
        sum(AMOUNT) as total_spent
    from joined_data
    group by CUSTOMER_ID
)
select
    CUSTOMER_ID,
    total_spent
from customer_spent_amount
order by total_spent desc
limit 3
