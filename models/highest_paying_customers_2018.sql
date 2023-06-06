
WITH orders_2018 AS (
    SELECT *
    FROM {{ ref('stg_orders') }}
    WHERE EXTRACT(YEAR FROM order_date) = 2018
),

joined_data AS (
    SELECT o.customer_id, p.amount
    FROM orders_2018 o
    JOIN {{ ref('stg_payments') }} p ON o.order_id = p.order_id
)

SELECT customer_id, SUM(amount) as total_payment
FROM joined_data
GROUP BY customer_id
ORDER BY total_payment DESC

