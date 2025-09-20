-- Customer Analytics C360 Data Product - Consolidated Pipeline
-- Description: Complete pipeline running in a single Spark session
-- Usage: spark-sql -f c360_consolidated_pipeline.sql

-- =============================================================================
-- STEP 1: CREATE RAW DATA VIEWS (CSV LOADING)
-- =============================================================================

-- Customer raw data
CREATE OR REPLACE TEMPORARY VIEW customers_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/customer/customers.csv",
  header "true",
  inferSchema "true"
);

-- Loyalty program raw data
CREATE OR REPLACE TEMPORARY VIEW loyalty_program_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/customer/loyalty_program.csv",
  header "true",
  inferSchema "true"
);

-- Transaction raw data
CREATE OR REPLACE TEMPORARY VIEW transactions_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/sales/transactions.csv",
  header "true",
  inferSchema "true"
);

-- Transaction items raw data
CREATE OR REPLACE TEMPORARY VIEW transaction_items_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/sales/transaction_items.csv",
  header "true",
  inferSchema "true"
);

-- Products raw data
CREATE OR REPLACE TEMPORARY VIEW products_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/products/products.csv",
  header "true",
  inferSchema "true"
);

-- Support tickets raw data
CREATE OR REPLACE TEMPORARY VIEW support_tickets_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/customer/support_tickets.csv",
  header "true",
  inferSchema "true"
);

-- App usage raw data
CREATE OR REPLACE TEMPORARY VIEW app_usage_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/customer/app_usage.csv",
  header "true",
  inferSchema "true"
);

-- =============================================================================
-- STEP 2: CREATE SOURCE VIEWS (ENRICHED DATA)
-- =============================================================================

-- Enriched customer view
CREATE OR REPLACE TEMPORARY VIEW src_customers AS
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    CAST(date_of_birth AS DATE) as date_of_birth,
    gender,
    CAST(registration_date AS TIMESTAMP) as registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    state,
    zip_code,
    country,
    -- Add derived fields
    DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 as age_years,
    DATEDIFF(CURRENT_DATE(), CAST(registration_date AS TIMESTAMP)) as days_since_registration,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 25 THEN 'Gen Z'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 40 THEN 'Millennial'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 55 THEN 'Gen X'
        ELSE 'Boomer+'
    END as generation_segment,
    -- Data quality flags
    CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END as missing_email_flag,
    CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END as missing_phone_flag
FROM customers_raw;

-- Enriched loyalty program view
CREATE OR REPLACE TEMPORARY VIEW src_loyalty_program AS
SELECT 
    customer_id,
    loyalty_tier,
    points_balance,
    points_earned_ytd,
    points_redeemed_ytd,
    CAST(tier_start_date AS DATE) as tier_start_date,
    lifetime_value,
    -- Derived metrics
    points_earned_ytd - points_redeemed_ytd as net_points_ytd,
    CAST(points_redeemed_ytd AS DOUBLE) / NULLIF(points_earned_ytd, 0) as redemption_rate,
    DATEDIFF(CURRENT_DATE(), CAST(tier_start_date AS DATE)) as days_in_current_tier,
    -- Tier rankings for analysis
    CASE loyalty_tier
        WHEN 'Bronze' THEN 1
        WHEN 'Silver' THEN 2  
        WHEN 'Gold' THEN 3
        WHEN 'Platinum' THEN 4
        ELSE 0
    END as tier_rank,
    -- Value segments based on lifetime value
    CASE 
        WHEN lifetime_value < 1000 THEN 'Low Value'
        WHEN lifetime_value < 5000 THEN 'Medium Value'
        WHEN lifetime_value < 10000 THEN 'High Value'
        ELSE 'VIP'
    END as value_segment
FROM loyalty_program_raw;

-- Enriched transactions view
CREATE OR REPLACE TEMPORARY VIEW src_transactions AS
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
    DATE(transaction_date) as transaction_date_only,
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
FROM transactions_raw
WHERE status = 'completed';

-- Enriched products view
CREATE OR REPLACE TEMPORARY VIEW src_products AS
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    weight_kg,
    dimensions,
    color,
    size,
    CAST(created_date AS DATE) as created_date,
    status,
    -- Derived metrics
    price - cost as gross_margin,
    (price - cost) / NULLIF(price, 0) as margin_percent,
    DATEDIFF(CURRENT_DATE(), CAST(created_date AS DATE)) as days_since_launch,
    -- Price segments
    CASE 
        WHEN price < 50 THEN 'Budget'
        WHEN price < 150 THEN 'Mid-Range'
        WHEN price < 300 THEN 'Premium'
        ELSE 'Luxury'
    END as price_segment,
    -- Category groupings for analysis
    CASE 
        WHEN category IN ('Electronics', 'Computer') THEN 'Technology'
        WHEN category IN ('Apparel', 'Footwear', 'Accessories') THEN 'Fashion'
        WHEN category IN ('Home & Kitchen', 'Home & Garden', 'Home & Office') THEN 'Home'
        WHEN category IN ('Sports & Fitness', 'Health & Nutrition') THEN 'Health & Fitness'
        WHEN category IN ('Beauty', 'Personal Care') THEN 'Beauty & Personal Care'
        ELSE 'Other'
    END as category_group
FROM products_raw
WHERE status = 'active';

-- =============================================================================
-- STEP 3: CREATE INTERMEDIATE VIEWS  
-- =============================================================================

-- Customer transaction integration
CREATE OR REPLACE TEMPORARY VIEW int_customer_transactions AS
WITH transaction_items AS (
    SELECT 
        transaction_id,
        product_id,
        quantity,
        unit_price,
        line_total,
        discount_applied
    FROM transaction_items_raw
),

transaction_summary AS (
    SELECT 
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.channel,
        t.channel_group,
        t.total_amount,
        t.discount_amount,
        t.order_size_category,
        t.transaction_hour,
        t.day_of_week,
        t.transaction_month,
        t.transaction_quarter,
        t.transaction_year,
        -- Item-level aggregations
        COUNT(ti.product_id) as items_purchased,
        SUM(ti.quantity) as total_quantity,
        AVG(ti.unit_price) as avg_item_price,
        -- Product category analysis
        COUNT(DISTINCT p.category) as unique_categories,
        COUNT(DISTINCT p.brand) as unique_brands,
        -- Category preferences
        COLLECT_LIST(p.category_group) as purchased_category_groups,
        COLLECT_LIST(p.brand) as purchased_brands
    FROM src_transactions t
    INNER JOIN transaction_items ti ON t.transaction_id = ti.transaction_id  
    INNER JOIN src_products p ON ti.product_id = p.product_id
    GROUP BY 
        t.transaction_id, t.customer_id, t.transaction_date, t.channel, 
        t.channel_group, t.total_amount, t.discount_amount, t.order_size_category,
        t.transaction_hour, t.day_of_week, t.transaction_month, 
        t.transaction_quarter, t.transaction_year
)

SELECT 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.customer_segment,
    c.preferred_channel,
    c.generation_segment,
    c.age_years,
    c.days_since_registration,
    -- Transaction details
    ts.transaction_id,
    ts.transaction_date,
    ts.channel,
    ts.channel_group,
    ts.total_amount,
    ts.discount_amount,
    ts.order_size_category,
    ts.items_purchased,
    ts.total_quantity,
    ts.avg_item_price,
    ts.unique_categories,
    ts.unique_brands,
    ts.purchased_category_groups,
    ts.purchased_brands,
    -- Time-based analysis
    ts.transaction_hour,
    ts.day_of_week,
    ts.transaction_month,
    ts.transaction_quarter,
    ts.transaction_year,
    -- Channel alignment analysis
    CASE WHEN c.preferred_channel = ts.channel THEN 1 ELSE 0 END as used_preferred_channel
FROM src_customers c
INNER JOIN transaction_summary ts ON c.customer_id = ts.customer_id;

-- =============================================================================
-- STEP 4: CREATE FACT TABLE
-- =============================================================================

-- Customer 360 Profile Fact Table
CREATE OR REPLACE TEMPORARY VIEW fct_customer_360_profile AS
WITH support_summary AS (
    SELECT 
        customer_id,
        COUNT(*) as total_tickets,
        COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets,
        AVG(CASE WHEN satisfaction_score IS NOT NULL THEN satisfaction_score END) as avg_satisfaction,
        MAX(CAST(created_date AS TIMESTAMP)) as last_ticket_date,
        COUNT(CASE WHEN priority = 'urgent' THEN 1 END) as urgent_tickets
    FROM support_tickets_raw
    GROUP BY customer_id
),

app_usage_summary AS (
    SELECT 
        customer_id,
        COUNT(*) as total_sessions,
        SUM(session_duration_minutes) as total_session_minutes,
        AVG(session_duration_minutes) as avg_session_duration,
        SUM(pages_viewed) as total_pages_viewed,
        SUM(actions_taken) as total_actions_taken,
        MAX(CAST(session_date AS DATE)) as last_app_use_date,
        COUNT(DISTINCT device_type) as unique_device_types,
        -- Engagement score based on activity
        (SUM(session_duration_minutes) * 0.3 + SUM(pages_viewed) * 0.4 + SUM(actions_taken) * 0.3) as engagement_score
    FROM app_usage_raw
    GROUP BY customer_id
),

transaction_metrics AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT transaction_id) as total_transactions,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_order_value,
        MIN(transaction_date) as first_purchase_date,
        MAX(transaction_date) as last_purchase_date,
        COUNT(DISTINCT channel) as channels_used,
        COUNT(DISTINCT DATE(transaction_date)) as shopping_days,
        SUM(CASE WHEN used_preferred_channel = 1 THEN 1 ELSE 0 END) as preferred_channel_usage,
        -- Category and brand diversity
        AVG(unique_categories) as avg_categories_per_order,
        AVG(unique_brands) as avg_brands_per_order,
        SUM(items_purchased) as total_items_purchased,
        -- Recent activity (last 90 days)
        COUNT(CASE WHEN DATEDIFF(CURRENT_DATE(), DATE(transaction_date)) <= 90 THEN 1 END) as transactions_last_90d,
        SUM(CASE WHEN DATEDIFF(CURRENT_DATE(), DATE(transaction_date)) <= 90 THEN total_amount ELSE 0 END) as spent_last_90d
    FROM int_customer_transactions
    GROUP BY customer_id
)

SELECT 
    -- Customer identifiers and demographics
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.customer_segment,
    c.preferred_channel,
    c.generation_segment,
    c.age_years,
    c.days_since_registration,
    c.city,
    c.state,
    c.country,
    
    -- Loyalty program data
    lp.loyalty_tier,
    lp.points_balance,
    lp.lifetime_value,
    lp.tier_rank,
    lp.value_segment,
    lp.redemption_rate,
    lp.days_in_current_tier,
    
    -- Transaction metrics
    COALESCE(tm.total_transactions, 0) as total_transactions,
    COALESCE(tm.total_spent, 0.0) as total_spent,
    COALESCE(tm.avg_order_value, 0.0) as avg_order_value,
    tm.first_purchase_date,
    tm.last_purchase_date,
    COALESCE(tm.channels_used, 0) as channels_used,
    COALESCE(tm.shopping_days, 0) as shopping_days,
    COALESCE(tm.preferred_channel_usage, 0) as preferred_channel_usage,
    COALESCE(tm.transactions_last_90d, 0) as transactions_last_90d,
    COALESCE(tm.spent_last_90d, 0.0) as spent_last_90d,
    
    -- Support metrics  
    COALESCE(ss.total_tickets, 0) as total_support_tickets,
    COALESCE(ss.resolved_tickets, 0) as resolved_support_tickets,
    ss.avg_satisfaction,
    ss.last_ticket_date,
    COALESCE(ss.urgent_tickets, 0) as urgent_support_tickets,
    
    -- App usage metrics
    COALESCE(aus.total_sessions, 0) as total_app_sessions,
    COALESCE(aus.total_session_minutes, 0) as total_session_minutes,
    aus.avg_session_duration,
    COALESCE(aus.total_pages_viewed, 0) as total_pages_viewed,
    COALESCE(aus.total_actions_taken, 0) as total_actions_taken,
    aus.last_app_use_date,
    COALESCE(aus.unique_device_types, 0) as unique_device_types,
    COALESCE(aus.engagement_score, 0.0) as app_engagement_score,
    
    -- Calculated customer health metrics
    CASE 
        WHEN tm.last_purchase_date IS NULL THEN 'Never Purchased'
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 30 THEN 'Active'
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as customer_status,
    
    -- RFM-like scoring
    CASE 
        WHEN tm.last_purchase_date IS NULL THEN 1
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 30 THEN 5
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 60 THEN 4
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 90 THEN 3
        WHEN DATEDIFF(CURRENT_DATE(), tm.last_purchase_date) <= 180 THEN 2
        ELSE 1
    END as recency_score,
    
    CASE 
        WHEN tm.total_transactions IS NULL OR tm.total_transactions = 0 THEN 1
        WHEN tm.total_transactions >= 10 THEN 5
        WHEN tm.total_transactions >= 5 THEN 4  
        WHEN tm.total_transactions >= 3 THEN 3
        WHEN tm.total_transactions >= 1 THEN 2
        ELSE 1
    END as frequency_score,
    
    CASE 
        WHEN tm.total_spent IS NULL OR tm.total_spent = 0 THEN 1
        WHEN tm.total_spent >= 1000 THEN 5
        WHEN tm.total_spent >= 500 THEN 4
        WHEN tm.total_spent >= 200 THEN 3  
        WHEN tm.total_spent >= 50 THEN 2
        ELSE 1
    END as monetary_score,
    
    CURRENT_TIMESTAMP() as profile_created_at

FROM src_customers c
LEFT JOIN src_loyalty_program lp ON c.customer_id = lp.customer_id
LEFT JOIN transaction_metrics tm ON c.customer_id = tm.customer_id  
LEFT JOIN support_summary ss ON c.customer_id = ss.customer_id
LEFT JOIN app_usage_summary aus ON c.customer_id = aus.customer_id;

-- =============================================================================
-- STEP 5: CREATE FINAL DATA PRODUCT VIEW
-- =============================================================================

-- Final Customer Analytics C360 Data Product
CREATE OR REPLACE TEMPORARY VIEW customer_analytics_c360 AS
SELECT 
    -- Primary identifiers
    customer_id,
    first_name,
    last_name,
    email,
    
    -- Demographics and segmentation
    customer_segment,
    preferred_channel,
    generation_segment,
    age_years,
    city,
    state,
    country,
    
    -- Account information
    days_since_registration,
    CASE 
        WHEN days_since_registration <= 30 THEN 'New (0-30 days)'
        WHEN days_since_registration <= 90 THEN 'Recent (31-90 days)'  
        WHEN days_since_registration <= 365 THEN 'Established (3-12 months)'
        ELSE 'Veteran (1+ years)'
    END as customer_tenure_segment,
    
    -- Loyalty program
    loyalty_tier,
    points_balance,
    lifetime_value,
    value_segment,
    redemption_rate,
    
    -- Purchase behavior
    total_transactions,
    total_spent,
    avg_order_value,
    first_purchase_date,
    last_purchase_date,
    channels_used,
    shopping_days,
    transactions_last_90d,
    spent_last_90d,
    
    -- Customer health metrics
    customer_status,
    recency_score,
    frequency_score, 
    monetary_score,
    
    -- Overall customer score (composite RFM)
    (recency_score + frequency_score + monetary_score) / 3.0 as customer_health_score,
    
    -- Engagement metrics
    total_app_sessions,
    total_session_minutes,
    avg_session_duration,
    app_engagement_score,
    last_app_use_date,
    unique_device_types,
    
    -- Support interaction
    total_support_tickets,
    resolved_support_tickets,
    avg_satisfaction,
    last_ticket_date,
    urgent_support_tickets,
    CASE 
        WHEN total_support_tickets = 0 THEN 'No Contact'
        WHEN avg_satisfaction >= 4.0 THEN 'Highly Satisfied'
        WHEN avg_satisfaction >= 3.0 THEN 'Satisfied'
        ELSE 'Needs Attention'
    END as support_satisfaction_level,
    
    -- Digital engagement flags
    CASE WHEN total_app_sessions > 0 THEN 1 ELSE 0 END as is_app_user,
    CASE WHEN preferred_channel IN ('online', 'mobile') THEN 1 ELSE 0 END as is_digital_native,
    
    -- Risk indicators
    CASE WHEN urgent_support_tickets > 0 THEN 1 ELSE 0 END as has_urgent_issues,
    CASE WHEN customer_status IN ('At Risk', 'Churned') THEN 1 ELSE 0 END as churn_risk_flag,
    CASE WHEN avg_satisfaction IS NOT NULL AND avg_satisfaction < 3.0 THEN 1 ELSE 0 END as satisfaction_risk_flag,
    
    -- Opportunity indicators  
    CASE WHEN channels_used = 1 AND total_transactions > 3 THEN 1 ELSE 0 END as channel_expansion_opportunity,
    CASE WHEN loyalty_tier IN ('Bronze', 'Silver') AND lifetime_value > 2000 THEN 1 ELSE 0 END as tier_upgrade_opportunity,
    CASE WHEN total_app_sessions = 0 AND customer_status = 'Active' THEN 1 ELSE 0 END as app_adoption_opportunity,
    
    -- Data freshness
    profile_created_at,
    CURRENT_TIMESTAMP() as view_accessed_at
    
FROM fct_customer_360_profile

-- Data quality filters
WHERE customer_id IS NOT NULL 
  AND email IS NOT NULL
  AND email != '';

-- =============================================================================
-- STEP 6: PIPELINE VALIDATION & RESULTS
-- =============================================================================

-- Show pipeline completion summary
SELECT 'Pipeline Summary' as stage, 
       COUNT(*) as total_customers,
       COUNT(CASE WHEN customer_status = 'Active' THEN 1 END) as active_customers,
       COUNT(CASE WHEN churn_risk_flag = 1 THEN 1 END) as at_risk_customers,
       ROUND(AVG(customer_health_score), 2) as avg_health_score
FROM customer_analytics_c360;

-- Show sample of top customers
SELECT 'Top 5 Customers by Health Score' as section,
       customer_id, 
       first_name,
       last_name,
       customer_segment,
       loyalty_tier,
       customer_status,
       total_spent,
       customer_health_score
FROM customer_analytics_c360 
ORDER BY customer_health_score DESC, total_spent DESC 
LIMIT 5;
