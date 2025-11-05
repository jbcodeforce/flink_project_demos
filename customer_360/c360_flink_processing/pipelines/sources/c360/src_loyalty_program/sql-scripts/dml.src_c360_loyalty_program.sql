INSERT INTO src_c360_loyalty_program
SELECT 
    customer_id,
    loyalty_tier,
    points_balance,
    points_earned_ytd,
    points_redeemed_ytd,
    tier_start_date,
    lifetime_value,
    -- Derived metrics
    points_earned_ytd - points_redeemed_ytd as net_points_ytd,
    CAST(points_redeemed_ytd AS DOUBLE) / NULLIF(points_earned_ytd, 0) as redemption_rate,
    TIMESTAMPDIFF(DAY, CAST(tier_start_date AS TIMESTAMP(3)), CURRENT_DATE) as days_in_current_tier,
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
FROM loyalty_program_raw