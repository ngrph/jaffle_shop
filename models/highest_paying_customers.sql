{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}

with orders as (
    select * from {{ ref('stg_orders') }}
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

customers as (
    select * from {{ ref('stg_customers') }}
),

customer_order_payments as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        {% for payment_method in payment_methods -%}
        sum(order_payments.{{ payment_method }}_amount) as {{ payment_method }}_total,
        {% endfor -%}
        sum(order_payments.total_amount) as customer_total_amount
    from customers
    left join orders on customers.customer_id = orders.customer_id
    left join order_payments on orders.order_id = order_payments.order_id
    group by customers.customer_id, customers.first_name, customers.last_name
),

final as (
    select * from customer_order_payments
    order by customer_total_amount desc
)

select * from final
