with orders_2018 as (
    select *
    from {{ ref('stg_orders') }}
    where order_date >= '2018-01-01' and order_date < '2019-01-01'
),

joined_data as (
    select
        o.customer_id,
        p.amount
    from orders_2018 o
    join {{ ref('stg_payments') }} p on o.order_id = p.order_id
),

total_payment_amount_2018 as (
    select
        customer_id,
        sum(amount) as total_payment_amount_2018
    from joined_data
    group by customer_id
)

select * from total_payment_amount_2018
