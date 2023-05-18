
with orders as (
    select * from {{ ref('stg_orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
)

select
    orders.*,
    customers.*,
    payments.*
from orders
join customers on orders.customer_id = customers.customer_id
join payments on orders.order_id = payments.order_id
