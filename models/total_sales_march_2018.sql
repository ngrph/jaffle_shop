
with march_orders as (
    select *
    from {{ ref('orders') }}
    where order_date between '2018-03-01' and '2018-03-31'
)
select sum(amount) as total_sales_march_2018
from march_orders

