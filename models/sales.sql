with orders as (
    select * from {{ ref('orders') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
payments as (
    select * from {{ ref('stg_payments') }}
),
combined as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        o.amount,
        c.first_name,
        c.last_name,
        p.payment_method
    from orders o
    join customers c on o.customer_id = c.customer_id
    join payments p on o.order_id = p.order_id
)

select * from combined
