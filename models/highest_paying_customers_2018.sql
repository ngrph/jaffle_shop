
WITH orders_2018 AS (
    SELECT *
    FROM {{ ref('orders') }}
    WHERE EXTRACT(YEAR FROM order_date) = 2018
),
joined_data AS (
    SELECT o.customer_id, c.first_name, c.last_name, o.amount
    FROM orders_2018 o
    JOIN {{ ref('customers') }} c ON o.customer_id = c.customer_id
)
SELECT customer_id, first_name, last_name, SUM(amount) as total_amount
FROM joined_data
GROUP BY customer_id, first_name, last_name
ORDER BY total_amount DESC

