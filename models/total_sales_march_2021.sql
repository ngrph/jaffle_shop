
WITH orders_march AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE order_date >= '2021-03-01' AND order_date <= '2021-03-31'
)

SELECT SUM(amount) as total_sales_march
FROM orders_march
