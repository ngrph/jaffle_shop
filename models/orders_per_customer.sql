{{
  config(
    materialized='table'
  )
}}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

joined_data as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        o.order_id
    from customers c
    join orders o
    on c.customer_id = o.customer_id
),

grouped_data as (
    select
        customer_id,
        first_name,
        last_name,
        count(order_id) as total_orders
    from joined_data
    group by customer_id, first_name, last_name
)

select * from grouped_data