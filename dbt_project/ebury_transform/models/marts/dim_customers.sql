-- models/marts/dim_customers.sql
SELECT
    DISTINCT customer_id,
    -- In a real scenario, you'd join with a customer metadata table here
    'Customer ' || customer_id::text AS customer_reference 
FROM {{ ref('stg_transactions') }}
