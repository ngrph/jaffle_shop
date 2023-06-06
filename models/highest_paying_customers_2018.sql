{% set year = 2018 %}

with orders_filtered as (
    select *
    from {{ ref('orders') }}
    where extract(year from order_date) = {{ year }}
),

customers_orders as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id,
        o.amount
    from {{ ref('customers') }} as c
    join orders_filtered as o
        on c.customer_id = o.customer_id
),

customer_total_amount as (
    select
        customer_id,
        first_name,
        last_name,
        sum(amount) as total_amount
    from customers_orders
    group by customer_id, first_name, last_name
)

select *
from customer_total_amount
order by total_amount desc
