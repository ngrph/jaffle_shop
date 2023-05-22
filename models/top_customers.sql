with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ ref('stg_payments') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined_data as (
    select
        c.customer_id,
        p.amount
    from customers c
    join orders o on c.customer_id = o.customer_id
    join payments p on o.order_id = p.order_id
),

customer_spend as (
    select
        customer_id,
        sum(amount) as total_amount_spent
    from joined_data
    group by customer_id
)

select * from customer_spend
order by total_amount_spent desc
limit 3