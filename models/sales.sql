{% set source_table = ref('orders') %}
SELECT
    ORDER_ID,
    CUSTOMER_ID,
    ORDER_DATE,
    AMOUNT AS SALES_AMOUNT
FROM {{ source_table }}