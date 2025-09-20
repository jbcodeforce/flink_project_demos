-- Customer Analytics C360 Data Product - Demo Queries
-- Run after executing the consolidated pipeline
-- Usage: First run c360_consolidated_pipeline.sql, then these queries

-- =============================================================================
-- DEMO 1: CUSTOMER HEALTH OVERVIEW
-- =============================================================================
SELECT 
    'Customer Health Overview' as demo,
    customer_status,
    COUNT(*) as customer_count,
    ROUND(AVG(customer_health_score), 2) as avg_health_score,
    ROUND(AVG(total_spent), 2) as avg_total_spent,
    ROUND(AVG(total_transactions), 1) as avg_transactions
FROM customer_analytics_c360
GROUP BY customer_status
ORDER BY customer_count DESC;

-- =============================================================================
-- DEMO 2: HIGH-VALUE CUSTOMERS AT RISK
-- =============================================================================
SELECT 
    'High-Value At-Risk Customers' as demo,
    customer_id,
    first_name || ' ' || last_name as customer_name,
    customer_segment,
    loyalty_tier,
    total_spent,
    last_purchase_date,
    customer_health_score,
    CASE 
        WHEN churn_risk_flag = 1 THEN '⚠️  CHURN RISK' 
        ELSE '✅ Stable'
    END as risk_status
FROM customer_analytics_c360
WHERE lifetime_value > 5000 
  AND churn_risk_flag = 1
ORDER BY total_spent DESC
LIMIT 10;

-- =============================================================================
-- DEMO 3: LOYALTY PROGRAM EFFECTIVENESS
-- =============================================================================
SELECT 
    'Loyalty Program Performance' as demo,
    loyalty_tier,
    COUNT(*) as customer_count,
    ROUND(AVG(lifetime_value), 2) as avg_lifetime_value,
    ROUND(AVG(total_spent), 2) as avg_total_spent,
    ROUND(AVG(points_balance), 0) as avg_points_balance,
    ROUND(AVG(redemption_rate) * 100, 1) as avg_redemption_rate_pct
FROM customer_analytics_c360
GROUP BY loyalty_tier
ORDER BY avg_lifetime_value DESC;

-- =============================================================================
-- DEMO 4: DIGITAL ENGAGEMENT ANALYSIS
-- =============================================================================
SELECT 
    'Digital Engagement Analysis' as demo,
    generation_segment,
    COUNT(*) as total_customers,
    SUM(is_app_user) as app_users,
    SUM(is_digital_native) as digital_natives,
    ROUND(AVG(app_engagement_score), 1) as avg_engagement_score,
    ROUND(SUM(is_app_user) * 100.0 / COUNT(*), 1) as app_adoption_rate_pct
FROM customer_analytics_c360
GROUP BY generation_segment
ORDER BY app_adoption_rate_pct DESC;

-- =============================================================================
-- DEMO 5: CROSS-SELL & UPSELL OPPORTUNITIES
-- =============================================================================
SELECT 
    'Cross-sell & Upsell Opportunities' as demo,
    customer_id,
    first_name || ' ' || last_name as customer_name,
    customer_segment,
    loyalty_tier,
    channels_used,
    total_spent,
    CASE WHEN channel_expansion_opportunity = 1 THEN '📱 Channel Expansion' ELSE '' END ||
    CASE WHEN tier_upgrade_opportunity = 1 THEN ' 🎖️  Tier Upgrade' ELSE '' END ||
    CASE WHEN app_adoption_opportunity = 1 THEN ' 📲 App Adoption' ELSE '' END as opportunities
FROM customer_analytics_c360
WHERE channel_expansion_opportunity = 1 
   OR tier_upgrade_opportunity = 1 
   OR app_adoption_opportunity = 1
ORDER BY total_spent DESC
LIMIT 10;

-- =============================================================================
-- DEMO 6: CUSTOMER SUPPORT SATISFACTION INSIGHTS
-- =============================================================================
SELECT 
    'Customer Support Insights' as demo,
    support_satisfaction_level,
    COUNT(*) as customer_count,
    ROUND(AVG(total_support_tickets), 1) as avg_tickets_per_customer,
    ROUND(AVG(avg_satisfaction), 2) as avg_satisfaction_score,
    SUM(urgent_support_tickets) as total_urgent_tickets,
    ROUND(AVG(total_spent), 2) as avg_customer_value
FROM customer_analytics_c360
WHERE total_support_tickets > 0
GROUP BY support_satisfaction_level
ORDER BY avg_customer_value DESC;

-- =============================================================================
-- DEMO 7: CUSTOMER LIFECYCLE ANALYSIS
-- =============================================================================
SELECT 
    'Customer Lifecycle Analysis' as demo,
    customer_tenure_segment,
    COUNT(*) as customer_count,
    ROUND(AVG(total_transactions), 1) as avg_transactions,
    ROUND(AVG(total_spent), 2) as avg_total_spent,
    ROUND(AVG(customer_health_score), 2) as avg_health_score,
    SUM(CASE WHEN customer_status = 'Active' THEN 1 ELSE 0 END) as active_customers
FROM customer_analytics_c360
GROUP BY customer_tenure_segment
ORDER BY 
    CASE customer_tenure_segment
        WHEN 'New (0-30 days)' THEN 1
        WHEN 'Recent (31-90 days)' THEN 2
        WHEN 'Established (3-12 months)' THEN 3
        WHEN 'Veteran (1+ years)' THEN 4
    END;

-- =============================================================================
-- DEMO 8: RFM SEGMENTATION MATRIX
-- =============================================================================
SELECT 
    'RFM Segmentation Matrix' as demo,
    recency_score,
    frequency_score,
    monetary_score,
    COUNT(*) as customer_count,
    ROUND(AVG(total_spent), 2) as avg_total_spent,
    -- Business segment interpretation
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN '🏆 Champions'
        WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 4 THEN '💎 Loyal Customers'
        WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score >= 3 THEN '💰 Big Spenders'
        WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score <= 2 THEN '🌟 Potential Loyalists'
        WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN '⚠️  At Risk'
        WHEN recency_score <= 2 AND frequency_score <= 2 THEN '😴 Hibernating'
        ELSE '🎯 Others'
    END as business_segment
FROM customer_analytics_c360
GROUP BY recency_score, frequency_score, monetary_score
ORDER BY recency_score DESC, frequency_score DESC, monetary_score DESC;

-- =============================================================================
-- SUMMARY: DATA PRODUCT HEALTH CHECK
-- =============================================================================
SELECT 
    'C360 Data Product Health Check' as summary,
    COUNT(*) as total_customers_processed,
    COUNT(CASE WHEN email IS NOT NULL THEN 1 END) as customers_with_email,
    COUNT(CASE WHEN total_transactions > 0 THEN 1 END) as customers_with_purchases,
    COUNT(CASE WHEN total_app_sessions > 0 THEN 1 END) as app_engaged_customers,
    COUNT(CASE WHEN total_support_tickets > 0 THEN 1 END) as customers_with_support_history,
    ROUND(AVG(customer_health_score), 2) as overall_health_score,
    MAX(profile_created_at) as last_profile_update
FROM customer_analytics_c360;
