{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (
    select * from {{ ref('stg_orders') }}
    where extract(year from order_date) = 2018
),

payments as (
    select * from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}
        sum(amount) as total_amount
    from payments
    group by order_id
),

final as (
    select
        orders.customer_id,
        sum(order_payments.total_amount) as total_amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
    group by orders.customer_id
)

select * from final
order by total_amount desc
