with customers_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id
    from {{ ref('stg_customers') }} as c
    join {{ ref('stg_orders') }} as o
        on c.customer_id = o.customer_id
),

customers_orders_payments as (
    select
        co.customer_id,
        co.first_name,
        co.last_name,
        co.order_id,
        p.amount
    from customers_orders as co
    join {{ ref('stg_payments') }} as p
        on co.order_id = p.order_id
),

total_purchases as (
    select
        customer_id,
        first_name,
        last_name,
        sum(amount) as total_purchase_value
    from customers_orders_payments
    group by customer_id, first_name, last_name
)

select * from total_purchases
order by total_purchase_value desc