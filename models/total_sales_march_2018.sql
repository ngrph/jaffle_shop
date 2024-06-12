
WITH orders_march_2018 AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE ORDER_DATE >= '2018-03-01' AND ORDER_DATE <= '2018-03-31'
)

SELECT SUM(AMOUNT) as total_sales
FROM orders_march_2018
