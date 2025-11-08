INSERT INTO src_c360_transactions
SELECT 
    transaction_id,
    customer_id,
    CAST(transaction_date AS TIMESTAMP) as transaction_date,
    channel,
    store_id,
    payment_method,
    subtotal,
    tax_amount,
    discount_amount,
    total_amount,
    currency,
    status,
    -- Derived fields
    CAST(transaction_date AS DATE) as transaction_date_only,
    HOUR(transaction_date) as transaction_hour,
    DAYOFWEEK(transaction_date) as day_of_week,
    MONTH(transaction_date) as transaction_month,
    QUARTER(transaction_date) as transaction_quarter,
    YEAR(transaction_date) as transaction_year,
    -- Channel groupings
    CASE 
        WHEN channel IN ('online', 'mobile') THEN 'Digital'
        WHEN channel = 'store' THEN 'Physical'
        ELSE 'Other'
    END as channel_group,
    -- Discount analysis
    CASE WHEN discount_amount > 0 THEN 1 ELSE 0 END as has_discount,
    CASE WHEN discount_amount > 0 THEN discount_amount / NULLIF(subtotal, 0) ELSE 0 END as discount_rate,
    -- Order size categories
    CASE 
        WHEN total_amount < 50 THEN 'Small'
        WHEN total_amount < 150 THEN 'Medium'
        WHEN total_amount < 300 THEN 'Large'
        ELSE 'XLarge'
    END as order_size_category
FROM tx_raw
WHERE status = 'completed';