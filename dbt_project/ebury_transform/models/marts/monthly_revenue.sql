-- models/marts/monthly_revenue.sql
SELECT
    DATE_TRUNC('month', transaction_date) AS fiscal_month,
    COUNT(transaction_id) AS total_transactions,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction_value
FROM {{ ref('fact_transactions') }}
GROUP BY 1
ORDER BY 1 DESC
