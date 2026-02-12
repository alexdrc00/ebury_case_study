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
    
    Data Quality Fixes -- Preserve NULLs for tracking:
    1. Transaction ID: Remove 'T' prefix, convert to integer
    2. Customer ID: Convert to integer, handle floats
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

flagged AS (
	-- DATA QUALITY FLAGS
        SELECT
		transaction_id AS raw_transaction_id,
		customer_id AS raw_customer_id,
		transaction_date AS raw_transaction_date,
		product_id AS raw_product_id,
		product_name AS raw_product_name,
		quantity AS raw_quantity,
		price AS raw_price,
		tax AS raw_tax,
		ingested_at AS raw_ingested_at,
		source_file AS raw_source_file,
                pipeline_run_id AS raw_pipeline_run_id,

		-- Transaction ID issues
		CASE
			WHEN transaction_id IS NULL OR TRIM(transaction_id) = '' THEN TRUE
			ELSE FALSE
		END AS is_missing_transaction_id,
		CASE
			WHEN transaction_id ~ '^T[0-9]+$' THEN TRUE
			ELSE FALSE
		END AS had_transaction_id_prefix,

		-- Customer ID issues
		CASE
			WHEN customer_id IS NULL OR TRIM(customer_id) = '' THEN TRUE
			ELSE FALSE
		END AS is_missing_customer_id,

		-- Date Issues
		CASE
			WHEN transaction_date IS NULL OR TRIM(transaction_date) = '' THEN TRUE
			ELSE FALSE
		END AS is_missing_transaction_date,
		CASE
			WHEN transaction_date IS NOT NULL
			AND (
				(transaction_date ~ '^\d{4}-\d{2}-\d{2}$' AND try_cast_date(transaction_date, 'YYYY-MM-DD') IS NULL)
				OR (transaction_date ~ '^\d{2}-\d{2}-\d{4}$' AND try_cast_date(transaction_date, 'DD-MM-YYYY') IS NULL)
				OR (transaction_date !~ '^\d{4}-\d{2}-\d{2}$' OR transaction_date !~ '^\d{2}-\d{2}-\d{4}$')
			)
			THEN TRUE
			ELSE FALSE
		END AS had_invalid_date,

		-- Product Issues
		CASE
			WHEN product_id ~ '^P[0-9]+$' THEN TRUE
			ELSE FALSE
		END AS had_product_id_prefix,

		-- Qty Issues
		CASE
			WHEN quantity IS NULL OR TRIM(quantity) = '' THEN TRUE
			WHEN NOT quantity ~ '^[0-9]+\.?[0-9]*$' THEN TRUE
			ELSE FALSE
		END AS had_invalid_quantity,

		-- Price Issues
		CASE
		    WHEN price ~ '[A-Za-z]' THEN TRUE
		    ELSE FALSE
		END AS had_text_in_price,

		-- Tax Issues
		CASE
		    WHEN tax ~ '[A-Za-z]' THEN TRUE
		    ELSE FALSE
		END AS had_text_in_tax
	FROM source
),

cleaned AS (
    SELECT
	*,
        -- TRANSACTION IDENTIFIER
        -- Fix: Remove 'T' prefix from transaction IDs like 'T1010'
        CASE 
            WHEN raw_transaction_id IS NULL THEN NULL
            WHEN raw_transaction_id ~ '^[0-9]+$' THEN raw_transaction_id::INTEGER
            WHEN raw_transaction_id ~ '^T[0-9]+$' THEN SUBSTRING(raw_transaction_id FROM 2)::INTEGER
            ELSE NULL  -- Invalid format, set to NULL for data quality tracking
        END AS transaction_id,
        
        -- CUSTOMER IDENTIFIER
        -- Fix: Convert string to integer, handle floats, preserve NULLs for orphan tracking
        CASE 
            WHEN raw_customer_id IS NULL OR TRIM(raw_customer_id) = '' THEN NULL
            WHEN raw_customer_id ~ '^[0-9]+\.?[0-9]*$' THEN SPLIT_PART(raw_customer_id, '.', 1)::INTEGER
            ELSE NULL
        END AS customer_id,
        
        -- TRANSACTION DATE
        -- Fix: Standardize multiple date formats
        -- Allowed Formats: YYYY-MM-DD, DD-MM-YYYY
        CASE 
            -- Format: YYYY-MM-DD
            WHEN raw_transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN 
                try_cast_date(raw_transaction_date, 'YYYY-MM-DD')
            
            -- Format: DD-MM-YYYY
            WHEN raw_transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN 
                try_cast_date(raw_transaction_date, 'DD-MM-YYYY')
            
            ELSE NULL  -- Invalid format
        END AS transaction_date,
        
        -- PRODUCT IDENTIFIER
        -- Fix: Remove 'P' prefix from product IDs like 'P100'
        CASE 
            WHEN raw_product_id IS NULL THEN NULL
            WHEN raw_product_id ~ '^[0-9]+$' THEN raw_product_id::INTEGER
            WHEN raw_product_id ~ '^P[0-9]+$' THEN SUBSTRING(raw_product_id FROM 2)::INTEGER
            ELSE NULL
        END AS product_id,
        
        -- PRODUCT NAME
        -- Clean: Trim whitespace, standardize casing
        UPPER(TRIM(raw_product_name)) AS product_name,
        
        -- QUANTITY
        -- Fix: Convert to integer, handle floats and text values
        CASE 
            WHEN raw_quantity IS NULL OR TRIM(raw_quantity) = '' THEN NULL
            WHEN raw_quantity ~ '^[0-9]+\.?[0-9]*$' THEN SPLIT_PART(raw_quantity, '.', 1)::INTEGER
            ELSE try_cast_numeric(raw_quantity)::INTEGER  -- Final safety net
        END AS quantity,
         
        -- PRICE
        -- Fix: Handle text values like 'Two Hundred', convert to numeric
        CASE 
            WHEN raw_price IS NULL OR TRIM(raw_price) = '' THEN NULL
            WHEN raw_price ~ '^[0-9]+\.?[0-9]*$' THEN raw_price::NUMERIC(10,2)
            ELSE try_cast_numeric(raw_price)  -- Final safety net
        END AS price,
         
        -- TAX
        -- Fix: Handle text values, convert to numeric 
        CASE 
            WHEN raw_tax IS NULL OR TRIM(raw_tax) = '' THEN NULL
            WHEN raw_tax ~ '^[0-9]+\.?[0-9]*$' THEN raw_tax::NUMERIC(10,2)
            ELSE try_cast_numeric(raw_tax)  -- Final safety net
        END AS tax,
        
        -- METADATA FIELDS
        -- Preserve for traceability and debugging
        raw_ingested_at AS ingested_at,
        raw_source_file AS source_file,
        raw_pipeline_run_id AS pipeline_run_id
        
    FROM flagged
),

-- CALCULATED FIELDS
-- Add derived business fields
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
