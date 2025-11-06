INSERT INTO c360_fct_customer_profile

WITH support_summary AS (
    SELECT 
        customer_id,
        COUNT(*) as total_tickets,
        COUNT(CASE WHEN status = 'resolved' THEN 1 END) as resolved_tickets,
        AVG(CASE WHEN satisfaction_score IS NOT NULL THEN satisfaction_score END) as avg_satisfaction,
        MAX(CAST(created_date AS TIMESTAMP)) as last_ticket_date,
        COUNT(CASE WHEN priority = 'urgent' THEN 1 END) as urgent_tickets
    FROM src_c360_support_ticket
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
    FROM src_c360_app_usage
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
        COUNT(DISTINCT transaction_date) as shopping_days,
        SUM(CASE WHEN used_preferred_channel = 1 THEN 1 ELSE 0 END) as preferred_channel_usage,
        -- Category and brand diversity
        AVG(unique_categories) as avg_categories_per_order,
        AVG(unique_brands) as avg_brands_per_order,
        SUM(items_purchased) as total_items_purchased,
        -- Recent activity (last 90 days)
        COUNT(CASE WHEN TIMESTAMPDIFF(DAY, CAST(transaction_date as TIMESTAMP_LTZ(3)), `$rowtime`) <= 90 THEN 1 END) as transactions_last_90d,
        SUM(CASE WHEN TIMESTAMPDIFF(DAY,  CAST(transaction_date as TIMESTAMP_LTZ(3)), `$rowtime`) <= 90 THEN total_amount ELSE 0 END) as spent_last_90d,
        `$rowtime` as current_ts
    FROM int_c360_customer_transactions
    GROUP BY customer_id, `$rowtime`
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
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 30 THEN 'Active'
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END as customer_status,
    
    -- RFM-like scoring
    CASE 
        WHEN tm.last_purchase_date IS NULL THEN 1
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 30 THEN 5
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 60 THEN 4
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 90 THEN 3
        WHEN TIMESTAMPDIFF(DAY, CAST(tm.last_purchase_date as TIMESTAMP_LTZ(3)), tm.current_ts) <= 180 THEN 2
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
   tm.current_ts as profile_created_at
FROM src_c360_customers c
LEFT JOIN src_c360_loyalty_program lp ON c.customer_id = lp.customer_id
LEFT JOIN transaction_metrics tm ON c.customer_id = tm.customer_id  
LEFT JOIN support_summary ss ON c.customer_id = ss.customer_id
LEFT JOIN app_usage_summary aus ON c.customer_id = aus.customer_id
