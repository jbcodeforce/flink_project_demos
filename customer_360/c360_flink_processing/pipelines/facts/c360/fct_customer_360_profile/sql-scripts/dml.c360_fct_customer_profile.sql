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

SELECT 
-- part to select stuff
FROM src_c360_customers c
LEFT JOIN src_loyalty_program lp ON c.customer_id = lp.customer_id
LEFT JOIN transaction_metrics tm ON c.customer_id = tm.customer_id  
LEFT JOIN support_summary ON c.customer_id = support_summary.customer_id
LEFT JOIN app_usage_summary ON c.customer_id = app_usage_summary.customer_id
