
WITH orders_march AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE order_date BETWEEN '2018-03-01' AND '2018-03-31'
)
SELECT SUM(amount) AS total_sales
FROM orders_march
