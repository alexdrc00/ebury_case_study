{{
    config(
        materialized='table',
        tags=['aggregate', 'reporting', 'mart']
    )
}}

/*
    Aggregate: Monthly Customer Summary
    
    Purpose:
    - Pre-aggregate customer metrics by month for fast reporting
    - Track customer behavior trends over time
    - Enable month-over-month analysis
    
    Grain: One row per customer per month
    
    Dependencies:
    - fact_transactions
    - dim_customers
    - dim_dates
*/

WITH monthly_transactions AS (
    SELECT
        f.customer_key,
        d.year,
        d.month,
        d.year_month,
        d.quarter,
        d.year_quarter,
        
        -- Transaction counts
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT f.date_key) AS active_days,
        
        -- Product diversity
        COUNT(DISTINCT f.product_key) AS unique_products_purchased,
        
        -- Quantity metrics
        SUM(f.quantity) AS total_quantity,
        AVG(f.quantity) AS avg_quantity_per_transaction,
        
        -- Revenue metrics
        SUM(f.line_subtotal) AS total_subtotal,
        SUM(f.tax_amount) AS total_tax,
        SUM(f.line_total) AS total_revenue,
        AVG(f.line_total) AS avg_transaction_value,
        MIN(f.line_total) AS min_transaction_value,
        MAX(f.line_total) AS max_transaction_value,
        
        -- Purchase behavior
        SUM(CASE WHEN f.is_bulk_purchase THEN 1 ELSE 0 END) AS bulk_purchase_count,
        SUM(CASE WHEN f.is_high_value_transaction THEN 1 ELSE 0 END) AS high_value_transaction_count,
        SUM(CASE WHEN f.is_premium_price THEN 1 ELSE 0 END) AS premium_price_count,
        
        -- Data quality
        AVG(f.data_quality_score) AS avg_data_quality_score,
        SUM(CASE WHEN f.is_clean_record THEN 1 ELSE 0 END) AS clean_record_count
        
    FROM {{ ref('fact_transactions') }} f
    INNER JOIN {{ ref('dim_dates') }} d ON f.date_key = d.date_day
    GROUP BY 
        f.customer_key,
        d.year,
        d.month,
        d.year_month,
        d.quarter,
        d.year_quarter
),

customer_context AS (
    SELECT
        customer_id,
        customer_name,
        is_unknown_customer,
        customer_value_segment,
        customer_tier
    FROM {{ ref('dim_customers') }}
),

enriched AS (
    SELECT
        m.*,
        c.customer_name,
        c.is_unknown_customer,
        c.customer_value_segment,
        c.customer_tier,
        
        -- Derived metrics
        ROUND(m.total_revenue / NULLIF(m.transaction_count, 0), 2) AS revenue_per_transaction,
        ROUND(m.total_revenue / NULLIF(m.active_days, 0), 2) AS revenue_per_active_day,
        ROUND(100.0 * m.bulk_purchase_count / NULLIF(m.transaction_count, 0), 2) AS bulk_purchase_rate,
        ROUND(100.0 * m.high_value_transaction_count / NULLIF(m.transaction_count, 0), 2) AS high_value_transaction_rate,
        
        -- Performance indicators
        CASE
            WHEN m.total_revenue > 500 THEN 'High Performer'
            WHEN m.total_revenue > 200 THEN 'Medium Performer'
            ELSE 'Low Performer'
        END AS monthly_performance_tier
        
    FROM monthly_transactions m
    LEFT JOIN customer_context c ON m.customer_key = c.customer_id
),

final AS (
    SELECT
        -- Composite key
        customer_key,
        year_month,
        
        -- Time dimensions
        year,
        month,
        quarter,
        year_quarter,
        
        -- Customer attributes
        customer_name,
        is_unknown_customer,
        customer_value_segment,
        customer_tier,
        
        -- Activity metrics
        transaction_count,
        active_days,
        unique_products_purchased,
        
        -- Volume metrics
        total_quantity,
        avg_quantity_per_transaction,
        
        -- Revenue metrics
        total_subtotal,
        total_tax,
        total_revenue,
        avg_transaction_value,
        min_transaction_value,
        max_transaction_value,
        revenue_per_transaction,
        revenue_per_active_day,
        
        -- Behavior metrics
        bulk_purchase_count,
        bulk_purchase_rate,
        high_value_transaction_count,
        high_value_transaction_rate,
        premium_price_count,
        
        -- Quality metrics
        avg_data_quality_score,
        clean_record_count,
        
        -- Performance classification
        monthly_performance_tier,
        
        -- Metadata
        CURRENT_TIMESTAMP AS agg_created_at
        
    FROM enriched
)

SELECT * FROM final
ORDER BY year_month DESC, total_revenue DESC
