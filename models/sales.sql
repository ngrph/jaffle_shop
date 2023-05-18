
{{ config(materialized='table') }}
SELECT
    order_date,
    SUM(amount) as total_amount
FROM {{ ref('orders') }}
GROUP BY order_date
