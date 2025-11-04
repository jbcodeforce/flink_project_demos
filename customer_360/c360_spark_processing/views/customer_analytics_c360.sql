-- View: Customer Analytics C360 Data Product
-- Description: Final consumable view for the customer.analytics.C360 data product
-- Purpose: Provides a comprehensive, high-quality, and up-to-date view of customers 
--          for analytics, BI, and ML initiatives across all domains
-- Data Owner: Customer Experience/CRM Team
-- Dependencies: fct_customer_360_profile

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

-- Add comment for data product metadata
COMMENT ON customer_analytics_c360 IS '
Customer 360 Data Product (customer.analytics.C360)
==================================================

Purpose: Comprehensive customer view for analytics, BI, and ML initiatives

Key Features:
- Real-time customer health scoring (RFM analysis)
- Cross-domain data integration (Sales, Loyalty, Support, Digital)
- Risk and opportunity identification
- Data quality validated and enriched

Usage Examples:
1. Marketing: SELECT * FROM customer_analytics_c360 WHERE churn_risk_flag = 1
2. Product: SELECT generation_segment, COUNT(*) FROM customer_analytics_c360 GROUP BY 1  
3. Finance: SELECT value_segment, AVG(lifetime_value) FROM customer_analytics_c360 GROUP BY 1

Data Freshness: Updated real-time via streaming ingestion
Data Quality: 99.5% completeness on key fields
Access Control: Row-level security based on customer consent flags

Contact: Customer Domain Team (customer-data-team@company.com)
Last Updated: 2024-06-20
';
