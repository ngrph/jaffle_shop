with customers as (
    select * from {{ ref('stg_customers') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

orders as (
    select * from {{ ref('orders') }}
    where date_part('year', order_date) = 2018
),

combined_data as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        orders.order_id,
        payments.amount
    from customers
    join orders
        on customers.customer_id = orders.customer_id
    join payments
        on orders.order_id = payments.order_id
),

grouped_data as (
    select
        customer_id,
        first_name,
        last_name,
        sum(amount) as total_amount_paid
    from combined_data
    group by customer_id, first_name, last_name
)

select * from grouped_data
order by total_amount_paid desc
