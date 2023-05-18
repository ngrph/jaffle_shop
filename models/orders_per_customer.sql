with orders as (
    select * from {{ ref('stg_orders') }}
),

grouped_orders as (
    select
        customer_id,
        count(order_id) as total_orders
    from orders
    group by customer_id
)

select * from grouped_orders