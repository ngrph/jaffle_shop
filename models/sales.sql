{% set orders = ref('orders') %}
SELECT
    ORDER_DATE,
    SUM(AMOUNT) as total_sales
FROM {{ orders }}
GROUP BY ORDER_DATE