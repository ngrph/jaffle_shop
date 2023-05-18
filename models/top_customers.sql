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
        c.customer_id,
        c.first_name,
        c.last_name,
        p.amount
    from customers c
    join orders o on c.customer_id = o.customer_id
    join payments p on o.order_id = p.order_id
),

grouped_data as (
    select
        customer_id,
        first_name,
        last_name,
        sum(amount) as total_spent
    from joined_data
    group by customer_id, first_name, last_name
)

select * from grouped_data
order by total_spent desc
limit 3