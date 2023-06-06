{% set start_date = '2018-01-01' %}
{% set end_date = '2018-12-31' %}

with orders as (
    select
        order_id,
        order_date
    from {{ ref('stg_orders') }}
),

payments as (
    select
        order_id,
        amount
    from {{ ref('stg_payments') }}
),

orders_payments as (
    select
        orders.order_id,
        orders.order_date,
        payments.amount
    from orders
    join payments
        on orders.order_id = payments.order_id
),

sales_2018 as (
    select
        order_date,
        sum(amount) as total_amount
    from orders_payments
    where order_date between '{{ start_date }}' and '{{ end_date }}'
    group by order_date
)

select sum(total_amount) as total_sales_2018 from sales_2018