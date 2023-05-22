
WITH payment_counts AS (
    SELECT
        PAYMENT_METHOD,
        COUNT(*) as payment_count
    FROM {{ ref('raw_payments') }}
    GROUP BY PAYMENT_METHOD
)
SELECT
    PAYMENT_METHOD,
    payment_count
FROM payment_counts
ORDER BY payment_count DESC
