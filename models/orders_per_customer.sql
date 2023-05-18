with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id
    from customers c
    join orders o on c.customer_id = o.customer_id
)

select
    customer_id,
    first_name,
    last_name,
    count(order_id) as total_orders
from joined
group by customer_id, first_name, last_name
