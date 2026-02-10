{{
    config(
        materialized='view',
        tags=['staging', 'data_quality']
    )
}}

/*
    Staging Model: Customer Transactions
    
    Purpose:
    - Clean and standardize raw transaction data
    - Handle data quality issues (nulls, format inconsistencies, type conversions)
    - Prepare data for dimensional modeling
    
    Data Quality Fixes:
    1. Transaction ID: Remove 'T' prefix, convert to integer
    2. Customer ID: Convert to integer, preserve NULLs for tracking
    3. Transaction Date: Standardize multiple date formats to DATE type
    4. Product ID: Remove 'P' prefix, convert to integer
    5. Quantity: Convert to integer, preserve NULLs
    6. Price: Handle text values, convert to numeric
    7. Tax: Handle text values, convert to numeric
    
    Dependencies:
    - raw.customer_transactions
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customer_transactions') }}
),

cleaned AS (
    SELECT
        -- ================================================================
        -- TRANSACTION IDENTIFIER
        -- Fix: Remove 'T' prefix from transaction IDs like 'T1010'
        -- ================================================================
        CASE 
            WHEN transaction_id IS NULL THEN NULL
            WHEN transaction_id ~ '^[0-9]+$' THEN transaction_id::INTEGER
            WHEN transaction_id ~ '^T[0-9]+$' THEN SUBSTRING(transaction_id FROM 2)::INTEGER
            ELSE NULL  -- Invalid format, set to NULL for data quality tracking
        END AS transaction_id,
        
        -- ================================================================
        -- CUSTOMER IDENTIFIER
        -- Fix: Convert string to integer, preserve NULLs for orphan tracking
        -- ================================================================
        CASE 
            WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN NULL
            WHEN customer_id ~ '^[0-9]+\.?[0-9]*$' THEN SPLIT_PART(customer_id, '.', 1)::INTEGER
            ELSE NULL
        END AS customer_id,
        
        -- ================================================================
        -- TRANSACTION DATE
        -- Fix: Standardize multiple date formats
        -- Formats: YYYY-MM-DD, DD-MM-YYYY
        -- ================================================================
        CASE 
            -- Format: YYYY-MM-DD (e.g., 2023-07-11)
            WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                transaction_date::DATE
            
            -- Format: DD-MM-YYYY (e.g., 18-07-2023)
            WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 
                TO_DATE(transaction_date, 'DD-MM-YYYY')
            
            ELSE NULL  -- Invalid format
        END AS transaction_date,
        
        -- ================================================================
        -- PRODUCT IDENTIFIER
        -- Fix: Remove 'P' prefix from product IDs like 'P100'
        -- ================================================================
        CASE 
            WHEN product_id IS NULL THEN NULL
            WHEN product_id ~ '^[0-9]+$' THEN product_id::INTEGER
            WHEN product_id ~ '^P[0-9]+$' THEN SUBSTRING(product_id FROM 2)::INTEGER
            ELSE NULL
        END AS product_id,
        
        -- ================================================================
        -- PRODUCT NAME
        -- Clean: Trim whitespace, standardize casing
        -- ================================================================
        TRIM(product_name) AS product_name,
        
        -- ================================================================
        -- QUANTITY
        -- Fix: Convert to integer, handle NULLs and text values
        -- ================================================================
        CASE 
            WHEN quantity IS NULL OR TRIM(quantity) = '' THEN NULL
            WHEN quantity ~ '^[0-9]+\.?[0-9]*$' THEN SPLIT_PART(quantity, '.', 1)::INTEGER
            ELSE NULL  -- Text values like 'Two Hundred' → NULL
        END AS quantity,
        
        -- ================================================================
        -- PRICE
        -- Fix: Handle text values like 'Two Hundred', convert to numeric
        -- ================================================================
        CASE 
            WHEN price IS NULL OR TRIM(price) = '' THEN NULL
            WHEN price ~ '^[0-9]+\.?[0-9]*$' THEN price::NUMERIC(10,2)
            -- Map known text values to numbers
            WHEN LOWER(TRIM(price)) = 'two hundred' THEN 200.00
            ELSE NULL
        END AS price,
        
        -- ================================================================
        -- TAX
        -- Fix: Handle text values like 'Fifteen', convert to numeric
        -- ================================================================
        CASE 
            WHEN tax IS NULL OR TRIM(tax) = '' THEN NULL
            WHEN tax ~ '^[0-9]+\.?[0-9]*$' THEN tax::NUMERIC(10,2)
            -- Map known text values to numbers
            WHEN LOWER(TRIM(tax)) = 'fifteen' THEN 15.00
            ELSE NULL
        END AS tax,
        
        -- ================================================================
        -- METADATA FIELDS
        -- Preserve for traceability and debugging
        -- ================================================================
        ingested_at,
        source_file,
        pipeline_run_id,
        
        -- ================================================================
        -- DATA QUALITY FLAGS
        -- Track which records had issues for monitoring
        -- ================================================================
        CASE 
            WHEN transaction_id ~ '^T[0-9]+$' THEN TRUE
            ELSE FALSE
        END AS had_transaction_id_prefix,
        
        CASE 
            WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN TRUE
            ELSE FALSE
        END AS is_missing_customer_id,
        
        CASE 
            WHEN quantity IS NULL OR TRIM(quantity) = '' THEN TRUE
            WHEN NOT quantity ~ '^[0-9]+\.?[0-9]*$' THEN TRUE
            ELSE FALSE
        END AS had_invalid_quantity,
        
        CASE 
            WHEN price ~ '[A-Za-z]' THEN TRUE
            ELSE FALSE
        END AS had_text_in_price,
        
        CASE 
            WHEN tax ~ '[A-Za-z]' THEN TRUE
            ELSE FALSE
        END AS had_text_in_tax

    FROM source
),

-- ================================================================
-- CALCULATED FIELDS
-- Add derived business fields
-- ================================================================
enriched AS (
    SELECT
        *,
        
        -- Calculate line total (subtotal before tax)
        CASE 
            WHEN quantity IS NOT NULL AND price IS NOT NULL THEN
                quantity * price
            ELSE NULL
        END AS line_subtotal,
        
        -- Calculate total amount (subtotal + tax)
        CASE 
            WHEN quantity IS NOT NULL AND price IS NOT NULL AND tax IS NOT NULL THEN
                (quantity * price) + tax
            ELSE NULL
        END AS line_total,
        
        -- Extract date components for time dimension
        EXTRACT(YEAR FROM transaction_date) AS transaction_year,
        EXTRACT(MONTH FROM transaction_date) AS transaction_month,
        EXTRACT(DAY FROM transaction_date) AS transaction_day,
        TO_CHAR(transaction_date, 'Day') AS transaction_day_name,
        TO_CHAR(transaction_date, 'Month') AS transaction_month_name,
        EXTRACT(QUARTER FROM transaction_date) AS transaction_quarter,
        
        -- Business flags
        CASE 
            WHEN quantity > 3 THEN TRUE
            ELSE FALSE
        END AS is_bulk_purchase

    FROM cleaned
)

SELECT * FROM enriched
