with orders as (
    select * from {{ ref('orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

sales as (
    select
        orders.order_id,
        orders.customer_id,
        customers.first_name,
        customers.last_name,
        orders.order_date,
        orders.status,
        orders.amount
    from orders
    join customers
        on orders.customer_id = customers.customer_id
)

select * from sales