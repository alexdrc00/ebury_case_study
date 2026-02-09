-- models/marts/fact_transactions.sql
SELECT
    transaction_id,
    customer_id,
    product_id,
    transaction_date,
    quantity,
    price,
    tax,
    total_amount
FROM {{ ref('stg_transactions') }}
