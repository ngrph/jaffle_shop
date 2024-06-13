
WITH orders_march AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE order_date >= '2018-03-01' AND order_date <= '2018-03-31'
)
SELECT SUM(amount) as total_sales
FROM orders_march
