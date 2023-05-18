with sales as (
    select
        o.*,
        c.*
    from {{ ref('orders') }} as o
    join {{ ref('stg_customers') }} as c
        on o.customer_id = c.customer_id
)

select * from sales