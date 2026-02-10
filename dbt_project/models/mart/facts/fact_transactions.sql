{{
    config(
        materialized='table',
        tags=['fact', 'mart']
    )
}}

/*
    Fact Table: Transactions
    
    Purpose:
    - Create transaction-level fact table for detailed analysis
    - Join with dimension tables via foreign keys
    - Include measures and degenerate dimensions
    
    Grain: One row per transaction
    
    Dependencies:
    - stg_transactions
    - dim_customers
    - dim_products
    - dim_dates
*/

WITH transactions AS (
    SELECT
        transaction_id,
        customer_id,
        product_id,
        transaction_date,
        quantity,
        price,
        tax,
        line_subtotal,
        line_total,
        product_name,
        is_bulk_purchase,
        
        -- Data quality flags
        had_transaction_id_prefix,
        is_missing_customer_id,
        had_invalid_quantity,
        had_text_in_price,
        had_text_in_tax,
        
        -- Metadata
        ingested_at,
        source_file,
        pipeline_run_id
        
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_id IS NOT NULL  -- Only include valid transactions
),

final AS (
    SELECT
        -- ================================================================
        -- PRIMARY KEY
        -- ================================================================
        t.transaction_id,
        
        -- ================================================================
        -- FOREIGN KEYS (Dimension References)
        -- ================================================================
        COALESCE(t.customer_id, -1) AS customer_key,  -- FK to dim_customers
        t.product_id AS product_key,                  -- FK to dim_products
        t.transaction_date AS date_key,               -- FK to dim_dates
        
        -- ================================================================
        -- DEGENERATE DIMENSIONS (Low-cardinality attributes)
        -- ================================================================
        t.product_name,
        
        -- ================================================================
        -- MEASURES (Numeric facts for aggregation)
        -- ================================================================
        
        -- Quantity measures
        COALESCE(t.quantity, 0) AS quantity,
        
        -- Price measures
        t.price AS unit_price,
        t.tax AS tax_amount,
        
        -- Revenue measures
        COALESCE(t.line_subtotal, 0) AS line_subtotal,
        COALESCE(t.line_total, 0) AS line_total,
        
        -- Calculated measures
        CASE 
            WHEN t.price > 0 THEN ROUND((t.tax / t.price) * 100, 2)
            ELSE NULL
        END AS tax_rate_percentage,
        
        CASE 
            WHEN t.line_subtotal > 0 THEN ROUND((t.tax / t.line_subtotal) * 100, 2)
            ELSE NULL
        END AS effective_tax_rate,
        
        -- Profit proxy (assuming fixed cost structure)
        CASE 
            WHEN t.line_subtotal > 0 THEN t.line_subtotal * 0.3  -- 30% margin assumption
            ELSE NULL
        END AS estimated_margin,
        
        -- ================================================================
        -- BUSINESS FLAGS (Boolean indicators for analysis)
        -- ================================================================
        t.is_bulk_purchase,
        
        CASE 
            WHEN t.customer_id IS NULL THEN TRUE
            ELSE FALSE
        END AS is_guest_transaction,
        
        CASE 
            WHEN t.quantity > 1 THEN TRUE
            ELSE FALSE
        END AS is_multi_item,
        
        CASE 
            WHEN t.price > 200 THEN TRUE
            ELSE FALSE
        END AS is_premium_price,
        
        CASE 
            WHEN t.line_total > 500 THEN TRUE
            ELSE FALSE
        END AS is_high_value_transaction,
        
        -- ================================================================
        -- DATA QUALITY FLAGS (For monitoring)
        -- ================================================================
        t.had_transaction_id_prefix,
        t.is_missing_customer_id,
        t.had_invalid_quantity,
        t.had_text_in_price,
        t.had_text_in_tax,
        
        -- Overall data quality score (0-5, where 5 is perfect)
        (5 -
            CASE WHEN t.had_transaction_id_prefix THEN 1 ELSE 0 END -
            CASE WHEN t.is_missing_customer_id THEN 1 ELSE 0 END -
            CASE WHEN t.had_invalid_quantity THEN 1 ELSE 0 END -
            CASE WHEN t.had_text_in_price THEN 1 ELSE 0 END -
            CASE WHEN t.had_text_in_tax THEN 1 ELSE 0 END
        ) AS data_quality_score,
        
        CASE 
            WHEN NOT (t.had_transaction_id_prefix OR 
                      t.is_missing_customer_id OR 
                      t.had_invalid_quantity OR 
                      t.had_text_in_price OR 
                      t.had_text_in_tax) THEN TRUE
            ELSE FALSE
        END AS is_clean_record,
        
        -- ================================================================
        -- METADATA (Lineage and audit fields)
        -- ================================================================
        ingested_at AS source_ingested_at,
        t.source_file,
        t.pipeline_run_id,
        CURRENT_TIMESTAMP AS fact_created_at
        
    FROM transactions t
)

SELECT * FROM final
ORDER BY transaction_id
