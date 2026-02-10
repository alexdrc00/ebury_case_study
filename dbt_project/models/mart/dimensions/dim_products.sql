{{
    config(
        materialized='table',
        tags=['dimension', 'mart']
    )
}}

/*
    Dimension: Products
    
    Purpose:
    - Create a product dimension with aggregated metrics
    - Track product performance and pricing
    
    Grain: One row per unique product
    
    Dependencies:
    - stg_transactions
*/

WITH product_transactions AS (
    SELECT
        product_id,
        product_name,
        quantity,
        price,
        tax,
        line_total,
        transaction_date
    FROM {{ ref('stg_transactions') }}
    WHERE transaction_id IS NOT NULL
        AND product_id IS NOT NULL
),

product_metrics AS (
    SELECT
        product_id,
        product_name,
        
        -- Sales volume metrics
        COUNT(*) AS total_transactions,
        SUM(COALESCE(quantity, 0)) AS total_quantity_sold,
        AVG(COALESCE(quantity, 0)) AS avg_quantity_per_transaction,
        
        -- Price metrics
        AVG(price) AS avg_price,
        MIN(price) AS min_price,
        MAX(price) AS max_price,
        STDDEV(price) AS price_stddev,
        MAX(price) - MIN(price) AS price_range,
        
        -- Tax metrics
        AVG(tax) AS avg_tax,
        SUM(tax) AS total_tax_collected,
        
        -- Revenue metrics
        SUM(COALESCE(line_total, 0)) AS total_revenue,
        AVG(COALESCE(line_total, 0)) AS avg_transaction_revenue,
        
        -- Temporal metrics
        MIN(transaction_date) AS first_sale_date,
        MAX(transaction_date) AS last_sale_date,
        COUNT(DISTINCT transaction_date) AS days_with_sales,
        
        -- Tax rate analysis
        AVG(CASE WHEN price > 0 THEN tax / price ELSE NULL END) AS avg_tax_rate
        
    FROM product_transactions
    GROUP BY product_id, product_name
),

final AS (
    SELECT
        product_id,
        product_name,
        
        -- Product category (derived from name pattern)
        CASE 
            WHEN product_name IN ('Product A', 'Product B') THEN 'Category 1'
            WHEN product_name IN ('Product C', 'Product D') THEN 'Category 2'
            ELSE 'Category 3'
        END AS product_category,
        
        -- Sales metrics
        total_transactions,
        total_quantity_sold,
        avg_quantity_per_transaction,
        
        -- Price metrics
        avg_price,
        min_price,
        max_price,
        price_stddev,
        price_range,
        
        -- Tax metrics
        avg_tax,
        total_tax_collected,
        ROUND(avg_tax_rate * 100, 2) AS avg_tax_rate_percentage,
        
        -- Revenue metrics
        total_revenue,
        avg_transaction_revenue,
        
        -- Performance metrics
        ROUND(total_revenue / NULLIF(total_transactions, 0), 2) AS revenue_per_transaction,
        ROUND(total_revenue / NULLIF(total_quantity_sold, 0), 2) AS revenue_per_unit,
        
        -- Temporal metrics
        first_sale_date,
        last_sale_date,
        days_with_sales,
        CURRENT_DATE - last_sale_date AS days_since_last_sale,
        
        -- Product performance segments
        CASE
            WHEN total_revenue > 3000 THEN 'Top Performer'
            WHEN total_revenue > 1500 THEN 'Good Performer'
            ELSE 'Low Performer'
        END AS performance_segment,
        
        CASE
            WHEN avg_price > 200 THEN 'Premium'
            WHEN avg_price > 100 THEN 'Mid-Range'
            ELSE 'Budget'
        END AS price_segment,
        
        CASE
            WHEN price_stddev / NULLIF(avg_price, 0) > 0.3 THEN 'High Variability'
            WHEN price_stddev / NULLIF(avg_price, 0) > 0.1 THEN 'Medium Variability'
            ELSE 'Low Variability'
        END AS price_stability,
        
        -- Flags
        CASE 
            WHEN CURRENT_DATE - last_sale_date > 7 THEN TRUE
            ELSE FALSE
        END AS is_slow_moving,
        
        -- Metadata
        CURRENT_TIMESTAMP AS dim_created_at,
        CURRENT_TIMESTAMP AS dim_updated_at
        
    FROM product_metrics
)

SELECT * FROM final
ORDER BY total_revenue DESC
