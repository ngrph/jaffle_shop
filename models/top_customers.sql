with customer_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id
    from {{ ref('stg_customers') }} as c
    join {{ ref('stg_orders') }} as o
    on c.customer_id = o.customer_id
),

customer_payments as (
    select
        co.customer_id,
        co.first_name,
        co.last_name,
        sum(p.amount) as lifetime_value
    from customer_orders as co
    join {{ ref('stg_payments') }} as p
    on co.order_id = p.order_id
    group by co.customer_id, co.first_name, co.last_name
)

select
    customer_id,
    first_name,
    last_name,
    lifetime_value
from customer_payments
order by lifetime_value desc
limit 3