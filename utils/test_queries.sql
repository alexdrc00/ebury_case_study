-- VERIFY DATA IN ALL LAYERS

SELECT 'raw' as layer, COUNT(*) as rows FROM raw.customer_transactions
UNION ALL
SELECT 'staging', COUNT(*) FROM staging.stg_transactions
UNION ALL
SELECT 'fact', COUNT(*) FROM mart.fact_transactions
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM mart.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM mart.dim_products
UNION ALL
SELECT 'dim_dates', COUNT(*) FROM mart.dim_dates;


-- CHECK DATA QUALITY

-- Data Quality Scores
SELECT
    data_quality_score,
    COUNT(*) as transaction_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM mart.fact_transactions
GROUP BY data_quality_score
ORDER BY data_quality_score DESC;

-- Data Quality Issues
SELECT
    COUNT(*) as total_transactions,
    SUM(CASE WHEN is_clean_record THEN 1 ELSE 0 END) as clean_records,
    SUM(CASE WHEN had_text_in_price THEN 1 ELSE 0 END) as had_text_in_price,
    SUM(CASE WHEN is_missing_customer_id THEN 1 ELSE 0 END) as missing_customer,
    ROUND(100.0 * SUM(CASE WHEN is_clean_record THEN 1 ELSE 0 END) / COUNT(*), 2) as clean_percentage
FROM mart.fact_transactions;


-- VERIFY DIMENSIONAL MODEL

-- Dimension Completeness
SELECT
    'Customers' as dimension,
    COUNT(*) as total,
    SUM(CASE WHEN is_unknown_customer THEN 1 ELSE 0 END) as unknown_count
FROM mart.dim_customers
UNION ALL
SELECT
    'Products',
    COUNT(*),
    0
FROM mart.dim_products
UNION ALL
SELECT
    'Dates',
    COUNT(*),
    0
FROM mart.dim_dates;

-- Foreign Keys (should equal 0)
SELECT COUNT(*) as orphaned_customers
FROM mart.fact_transactions f
LEFT JOIN mart.dim_customers c ON f.customer_key = c.customer_id
WHERE c.customer_id IS NULL;

SELECT COUNT(*) as orphaned_products
FROM mart.fact_transactions f
LEFT JOIN mart.dim_products p ON f.product_key = p.product_id
WHERE p.product_id IS NULL;


-- TEST BUSINESS QUERIES

-- Top 5 customers by revenue
SELECT
    customer_name,
    total_revenue,
    total_transactions,
    customer_value_segment,
    customer_tier
FROM mart.dim_customers
WHERE NOT is_unknown_customer
ORDER BY total_revenue DESC
LIMIT 5;

-- Product performance
SELECT
    product_name,
    product_category,
    total_revenue,
    total_quantity_sold,
    performance_segment
FROM mart.dim_products
ORDER BY total_revenue DESC;

-- Monthly revenue trend
SELECT
    year_month,
    SUM(total_revenue) as monthly_revenue,
    SUM(transaction_count) as total_transactions,
    COUNT(DISTINCT customer_key) as active_customers
FROM mart.monthly_customer_summary
GROUP BY year_month
ORDER BY year_month;

-- Weekend vs weekday analysis
SELECT
    d.is_weekend,
    COUNT(*) as transaction_count,
    SUM(f.line_total) as total_revenue,
    AVG(f.line_total) as avg_transaction_value
FROM mart.fact_transactions f
JOIN mart.dim_dates d ON f.date_key = d.date_day
GROUP BY d.is_weekend;
