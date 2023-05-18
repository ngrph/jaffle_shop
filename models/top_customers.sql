with payments_orders as (
    select
        p.payment_id,
        p.order_id,
        p.amount,
        o.customer_id
    from {{ ref('stg_payments') }} as p
    join {{ ref('stg_orders') }} as o
    on p.order_id = o.order_id
),

customer_spend as (
    select
        c.customer_id,
        sum(po.amount) as total_spent
    from {{ ref('stg_customers') }} as c
    join payments_orders as po
    on c.customer_id = po.customer_id
    group by c.customer_id
)

select
    customer_id,
    total_spent
from customer_spend
order by total_spent desc
limit 3