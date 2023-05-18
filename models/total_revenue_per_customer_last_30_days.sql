with last_30_days_revenue as (
    select
        customer_id,
        sum(amount) as total_revenue
    from {{ ref('orders') }}
    where order_date >= current_date - interval '30 days'
    group by customer_id
)

select * from last_30_days_revenue