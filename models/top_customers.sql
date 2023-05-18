
with orders_customers as (
    select
        o.order_id,
        o.customer_id,
        c.first_name || ' ' || c.last_name as customer_name
    from {{ ref('stg_orders') }} as o
    join {{ ref('stg_customers') }} as c
    on o.customer_id = c.customer_id
),
customer_payments as (
    select
        oc.customer_id,
        oc.customer_name,
        sum(p.amount) as total_spent
    from orders_customers as oc
    join {{ ref('stg_payments') }} as p
    on oc.order_id = p.order_id
    group by oc.customer_id, oc.customer_name
),
top_customers as (
    select
        customer_id,
        customer_name,
        total_spent
    from customer_payments
    order by total_spent desc
    limit 3
)
select * from top_customers
