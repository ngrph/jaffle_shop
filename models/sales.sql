with orders as (
    select * from {{ ref('orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

sales as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        payments.payment_method,
        payments.amount
    from orders
    left join payments
        on orders.order_id = payments.order_id
)

select * from sales