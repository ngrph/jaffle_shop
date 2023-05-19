with sales as (
    select
        order_id,
        customer_id,
        order_date,
        amount
    from {{ ref('orders') }}
)

select * from sales