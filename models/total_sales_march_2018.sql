
WITH filtered_orders AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE order_date >= '2018-03-01' AND order_date <= '2018-03-31'
)
SELECT SUM(amount) AS total_sales
FROM filtered_orders
