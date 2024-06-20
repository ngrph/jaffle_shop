
WITH orders_2018 AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE ORDER_DATE >= '2018-01-01' AND ORDER_DATE <= '2018-12-31'
)
SELECT SUM(AMOUNT) as total_sales_2018
FROM orders_2018
