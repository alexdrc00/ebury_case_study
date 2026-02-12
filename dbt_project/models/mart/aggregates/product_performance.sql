{{
    config(
        materialized='table',
        tags=['aggregate', 'reporting', 'mart']
    )
}}

/*
    Aggregate: Product Performance
    
    Purpose:
    - Analyze product performance across multiple dimensions
    - Enable product comparison and ranking
    - Support inventory and pricing decisions
    
    Grain: One row per product per time period (daily, weekly, monthly)
    
    Dependencies:
    - fact_transactions
    - dim_products
    - dim_dates
*/

WITH daily_product_metrics AS (
    SELECT
        f.product_key,
        f.date_key,
        d.year,
        d.month,
        d.quarter,
        d.year_month,
        d.week_of_year,
        d.day_name,
        d.is_weekend,
        
        -- Transaction metrics
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT f.customer_key) AS unique_customers,
        
        -- Volume metrics
        SUM(f.quantity) AS total_quantity,
        AVG(f.quantity) AS avg_quantity_per_transaction,
        
        -- Revenue metrics
        SUM(f.line_subtotal) AS total_subtotal,
        SUM(f.tax_amount) AS total_tax,
        SUM(f.line_total) AS total_revenue,
        AVG(f.unit_price) AS avg_unit_price,
        MIN(f.unit_price) AS min_unit_price,
        MAX(f.unit_price) AS max_unit_price,
        
        -- Margin metrics (estimated)
        SUM(f.estimated_margin) AS total_estimated_margin,
        
        -- Purchase patterns
        SUM(CASE WHEN f.is_bulk_purchase THEN 1 ELSE 0 END) AS bulk_purchase_count,
        SUM(CASE WHEN f.is_guest_transaction THEN 1 ELSE 0 END) AS guest_purchase_count
        
    FROM {{ ref('fact_transactions') }} f
    INNER JOIN {{ ref('dim_dates') }} d ON f.date_key = d.date_day
    GROUP BY 
        f.product_key,
        f.date_key,
        d.year,
        d.month,
        d.quarter,
        d.year_month,
        d.week_of_year,
        d.day_name,
        d.is_weekend
),

product_context AS (
    SELECT
        product_id,
        product_name,
        product_category,
        performance_segment,
        price_segment,
        price_stability
    FROM {{ ref('dim_products') }}
),

enriched AS (
    SELECT
        m.*,
        p.product_name,
        p.product_category,
        p.performance_segment,
        p.price_segment,
        p.price_stability,
        
        -- Calculated metrics
        ROUND(m.total_revenue / NULLIF(m.transaction_count, 0), 2) AS revenue_per_transaction,
        ROUND(m.total_revenue / NULLIF(m.total_quantity, 0), 2) AS revenue_per_unit,
        ROUND(m.total_estimated_margin / NULLIF(m.total_revenue, 0) * 100, 2) AS margin_percentage,
        ROUND(100.0 * m.bulk_purchase_count / NULLIF(m.transaction_count, 0), 2) AS bulk_purchase_rate,
        ROUND(100.0 * m.guest_purchase_count / NULLIF(m.transaction_count, 0), 2) AS guest_purchase_rate,
        
        -- Price variance
        m.max_unit_price - m.min_unit_price AS price_variance,
        CASE 
            WHEN m.avg_unit_price > 0 THEN
                ROUND((m.max_unit_price - m.min_unit_price) / m.avg_unit_price * 100, 2)
            ELSE NULL
        END AS price_variance_percentage
        
    FROM daily_product_metrics m
    LEFT JOIN product_context p ON m.product_key = p.product_id
),

-- Rolling metrics (7-day window)
with_rolling_metrics AS (
    SELECT
        *,
        
        -- 7-day rolling averages
        AVG(total_revenue) OVER (
            PARTITION BY product_key 
            ORDER BY date_key 
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ) AS revenue_7day_avg,
        
        AVG(total_quantity) OVER (
            PARTITION BY product_key 
            ORDER BY date_key 
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ) AS quantity_7day_avg,
        
        AVG(transaction_count) OVER (
            PARTITION BY product_key 
            ORDER BY date_key 
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW
        ) AS transaction_count_7day_avg,
        
        -- Rank products by revenue within each day
        RANK() OVER (
            PARTITION BY date_key 
            ORDER BY total_revenue DESC
        ) AS daily_revenue_rank
        
    FROM enriched
),

final AS (
    SELECT
        -- Composite key
        product_key,
        date_key,
        
        -- Product attributes
        product_name,
        product_category,
        performance_segment,
        price_segment,
        price_stability,
        
        -- Time dimensions
        year,
        month,
        quarter,
        year_month,
        week_of_year,
        day_name,
        is_weekend,
        
        -- Transaction metrics
        transaction_count,
        unique_customers,
        
        -- Volume metrics
        total_quantity,
        avg_quantity_per_transaction,
        
        -- Revenue metrics
        total_subtotal,
        total_tax,
        total_revenue,
        avg_unit_price,
        min_unit_price,
        max_unit_price,
        revenue_per_transaction,
        revenue_per_unit,
        
        -- Margin metrics
        total_estimated_margin,
        margin_percentage,
        
        -- Price metrics
        price_variance,
        price_variance_percentage,
        
        -- Purchase patterns
        bulk_purchase_count,
        bulk_purchase_rate,
        guest_purchase_count,
        guest_purchase_rate,
        
        -- Rolling metrics
        ROUND(revenue_7day_avg, 2) AS revenue_7day_avg,
        ROUND(quantity_7day_avg, 2) AS quantity_7day_avg,
        ROUND(transaction_count_7day_avg, 2) AS transaction_count_7day_avg,
        
        -- Rankings
        daily_revenue_rank,
        
        -- Performance indicators
        CASE
            WHEN total_revenue > revenue_7day_avg * 1.2 THEN 'Above Trend'
            WHEN total_revenue < revenue_7day_avg * 0.8 THEN 'Below Trend'
            ELSE 'On Trend'
        END AS trend_indicator,
        
        CASE
            WHEN daily_revenue_rank <= 2 THEN 'Top Performer'
            WHEN daily_revenue_rank <= 4 THEN 'Good Performer'
            ELSE 'Standard Performer'
        END AS daily_performance_tier,
        
        -- Metadata
        CURRENT_TIMESTAMP AS agg_created_at
        
    FROM with_rolling_metrics
)

SELECT * FROM final
ORDER BY date_key DESC, total_revenue DESC
