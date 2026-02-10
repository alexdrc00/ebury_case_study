{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

/*
    Dimension: Customers
    
    Purpose:
    - Create a Type 1 SCD (slowly changing dimension) for customers
    - Aggregate customer-level metrics for analysis
    - Handle NULL customer_ids as 'Unknown Customer'
    
    Grain: One row per unique customer
    
    Dependencies:
    - stg_transactions
*/

WITH customer_transactions AS (
    SELECT
        customer_id,
        transaction_date,
        line_total,
        is_bulk_purchase
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_id IS NOT NULL  -- Only valid transactions
),

customer_metrics AS (
    SELECT
        COALESCE(customer_id, -1) AS customer_id,  -- -1 for unknown customers
        
        -- Temporal metrics
        MIN(transaction_date) AS first_transaction_date,
        MAX(transaction_date) AS last_transaction_date,
        MAX(transaction_date) - MIN(transaction_date) AS customer_tenure_days,
        
        -- Transaction counts
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) AS active_months,
        COUNT(DISTINCT transaction_date) AS active_days,
        
        -- Purchase behavior
        COUNT(CASE WHEN is_bulk_purchase THEN 1 END) AS bulk_purchase_count,
        ROUND(
            100.0 * COUNT(CASE WHEN is_bulk_purchase THEN 1 END) / COUNT(*),
            2
        ) AS bulk_purchase_percentage,
        
        -- Revenue metrics
        SUM(COALESCE(line_total, 0)) AS total_revenue,
        AVG(COALESCE(line_total, 0)) AS avg_transaction_value,
        MIN(COALESCE(line_total, 0)) AS min_transaction_value,
        MAX(COALESCE(line_total, 0)) AS max_transaction_value,
        STDDEV(COALESCE(line_total, 0)) AS transaction_value_stddev,
        
        -- Recency
        CURRENT_DATE - MAX(transaction_date) AS days_since_last_transaction
        
    FROM customer_transactions
    GROUP BY COALESCE(customer_id, -1)
),

final AS (
    SELECT
        customer_id,
        
        -- Customer attributes
        CASE 
            WHEN customer_id = -1 THEN 'Unknown Customer'
            ELSE 'Customer ' || customer_id
        END AS customer_name,
        
        CASE 
            WHEN customer_id = -1 THEN TRUE
            ELSE FALSE
        END AS is_unknown_customer,
        
        -- Temporal attributes
        first_transaction_date,
        last_transaction_date,
        customer_tenure_days,
        
        -- Activity metrics
        total_transactions,
        active_months,
        active_days,
        
        -- Purchase behavior
        bulk_purchase_count,
        bulk_purchase_percentage,
        
        -- Revenue metrics
        total_revenue,
        avg_transaction_value,
        min_transaction_value,
        max_transaction_value,
        transaction_value_stddev,
        
        -- Recency metrics
        days_since_last_transaction,
        
        -- Customer segmentation
        CASE
            WHEN days_since_last_transaction <= 7 THEN 'Active'
            WHEN days_since_last_transaction <= 14 THEN 'At Risk'
            ELSE 'Inactive'
        END AS customer_status,
        
        CASE
            WHEN total_revenue > 1500 THEN 'High Value'
            WHEN total_revenue > 800 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS customer_value_segment,
        
        CASE
            WHEN avg_transaction_value > 200 THEN 'Premium'
            WHEN avg_transaction_value > 100 THEN 'Standard'
            ELSE 'Budget'
        END AS customer_tier,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dim_created_at,
        CURRENT_TIMESTAMP AS dim_updated_at
        
    FROM customer_metrics
)

SELECT * FROM final
ORDER BY customer_id
