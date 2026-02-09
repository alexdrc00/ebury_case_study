-- models/staging/stg_transactions.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_transactions') }}
)

SELECT
    CAST(transaction_id AS INTEGER) AS transaction_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(transaction_date AS DATE) AS transaction_date, -- Standardization [cite: 22, 61]
    CAST(product_id AS INTEGER) AS product_id,
    TRIM(product_name) AS product_name, -- Cleaning whitespace
    CAST(quantity AS INTEGER) AS quantity,
    CAST(price AS FLOAT) AS price,
    CAST(tax AS FLOAT) AS tax,
    (quantity * price) + tax AS total_amount -- Derived field for downstream use 
FROM source
