with orders_customers as (
    select
        o.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.amount
    from {{ ref('orders') }} as o
    join {{ ref('stg_customers') }} as c
        on o.customer_id = c.customer_id
),

aggregated as (
    select
        customer_id,
        first_name,
        last_name,
        sum(amount) as total_revenue,
        count(order_id) as total_orders,
        sum(amount) / count(order_id) as avg_order_value
    from orders_customers
    group by customer_id, first_name, last_name
)

select * from aggregated