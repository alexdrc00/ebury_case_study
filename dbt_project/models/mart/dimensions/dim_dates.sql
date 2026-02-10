{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

/*
    Dimension: Dates
    
    Purpose:
    - Create a comprehensive date dimension for time-based analysis
    - Pre-calculate date attributes to avoid runtime calculations
    
    Grain: One row per unique date in the transaction history
    
    Dependencies:
    - stg_transactions
*/

WITH transaction_dates AS (
    SELECT DISTINCT transaction_date AS date_day
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_date IS NOT NULL
),

date_spine AS (
    -- Generate a complete date range from min to max transaction date
    SELECT
        date_day
    FROM transaction_dates
    
    UNION
    
    -- Add surrounding dates for completeness
    SELECT
        MIN(date_day) - INTERVAL '7 days' AS date_day
    FROM transaction_dates
    
    UNION
    
    SELECT
        MAX(date_day) + INTERVAL '7 days' AS date_day
    FROM transaction_dates
),

date_dimension AS (
    SELECT
        date_day,
        
        -- Date components
        EXTRACT(YEAR FROM date_day) AS year,
        EXTRACT(MONTH FROM date_day) AS month,
        EXTRACT(DAY FROM date_day) AS day,
        EXTRACT(QUARTER FROM date_day) AS quarter,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        EXTRACT(DOW FROM date_day) AS day_of_week,  -- 0 = Sunday
        EXTRACT(DOY FROM date_day) AS day_of_year,
        
        -- Formatted strings
        TO_CHAR(date_day, 'Day') AS day_name,
        TO_CHAR(date_day, 'Dy') AS day_name_short,
        TO_CHAR(date_day, 'Month') AS month_name,
        TO_CHAR(date_day, 'Mon') AS month_name_short,
        TO_CHAR(date_day, 'YYYY-MM') AS year_month,
        TO_CHAR(date_day, 'YYYY-"Q"Q') AS year_quarter,
        TO_CHAR(date_day, 'YYYY-Www') AS year_week,
        
        -- Week start/end dates (Monday as week start)
        DATE_TRUNC('week', date_day)::DATE AS week_start_date,
        (DATE_TRUNC('week', date_day) + INTERVAL '6 days')::DATE AS week_end_date,
        
        -- Month start/end dates
        DATE_TRUNC('month', date_day)::DATE AS month_start_date,
        (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE AS month_end_date,
        
        -- Quarter start/end dates
        DATE_TRUNC('quarter', date_day)::DATE AS quarter_start_date,
        (DATE_TRUNC('quarter', date_day) + INTERVAL '3 months' - INTERVAL '1 day')::DATE AS quarter_end_date,
        
        -- Year start/end dates
        DATE_TRUNC('year', date_day)::DATE AS year_start_date,
        (DATE_TRUNC('year', date_day) + INTERVAL '1 year' - INTERVAL '1 day')::DATE AS year_end_date,
        
        -- Flags
        CASE 
            WHEN EXTRACT(DOW FROM date_day) IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekend,
        
        CASE 
            WHEN EXTRACT(DOW FROM date_day) NOT IN (0, 6) THEN TRUE
            ELSE FALSE
        END AS is_weekday,
        
        CASE 
            WHEN EXTRACT(DAY FROM date_day) = 1 THEN TRUE
            ELSE FALSE
        END AS is_month_start,
        
        CASE 
            WHEN date_day = (DATE_TRUNC('month', date_day) + INTERVAL '1 month' - INTERVAL '1 day')::DATE THEN TRUE
            ELSE FALSE
        END AS is_month_end,
        
        CASE 
            WHEN EXTRACT(MONTH FROM date_day) = EXTRACT(MONTH FROM CURRENT_DATE)
                AND EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE) THEN TRUE
            ELSE FALSE
        END AS is_current_month,
        
        CASE 
            WHEN EXTRACT(YEAR FROM date_day) = EXTRACT(YEAR FROM CURRENT_DATE) THEN TRUE
            ELSE FALSE
        END AS is_current_year,
        
        -- Relative period indicators
        CASE
            WHEN date_day = CURRENT_DATE THEN 'Today'
            WHEN date_day = CURRENT_DATE - 1 THEN 'Yesterday'
            WHEN date_day > CURRENT_DATE - 7 AND date_day < CURRENT_DATE THEN 'Last 7 Days'
            WHEN date_day > CURRENT_DATE - 30 AND date_day < CURRENT_DATE THEN 'Last 30 Days'
            WHEN date_day > CURRENT_DATE THEN 'Future'
            ELSE 'Historical'
        END AS relative_period,
        
        -- Days calculations
        CURRENT_DATE - date_day AS days_ago,
        date_day - CURRENT_DATE AS days_ahead,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dim_created_at,
        CURRENT_TIMESTAMP AS dim_updated_at
        
    FROM date_spine
)

SELECT * FROM date_dimension
ORDER BY date_day
