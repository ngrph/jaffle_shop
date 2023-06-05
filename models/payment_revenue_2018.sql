with source as (
    select
        p.payment_method,
        p.amount,
        o.order_date as created_at
    from {{ ref('stg_payments') }} as p
    join {{ ref('stg_orders') }} as o
    on p.order_id = o.order_id
),

filtered_2018 as (
    select
        payment_method,
        amount,
        date_trunc('year', created_at) as year
    from source
    where date_trunc('year', created_at) = '2018-01-01'
),

aggregated as (
    select
        payment_method,
        sum(amount) as total_revenue
    from filtered_2018
    group by payment_method
)

select * from aggregated
