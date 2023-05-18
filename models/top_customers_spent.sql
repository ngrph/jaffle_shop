with customer_orders as (
    select
        c.customer_id,
        o.order_id
    from {{ ref('stg_customers') }} as c
    join {{ ref('stg_orders') }} as o
    on c.customer_id = o.customer_id
),

customer_payments as (
    select
        co.customer_id,
        sum(p.amount) as total_spent
    from customer_orders as co
    join {{ ref('stg_payments') }} as p
    on co.order_id = p.order_id
    group by co.customer_id
),

top_customers as (
    select
        customer_id,
        total_spent
    from customer_payments
    order by total_spent desc
    limit 3
)

select * from top_customers