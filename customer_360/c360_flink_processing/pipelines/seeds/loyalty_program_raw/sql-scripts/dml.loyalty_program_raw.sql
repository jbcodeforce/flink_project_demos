INSERT INTO loyalty_program_raw (
    customer_id,
    loyalty_tier,
    points_balance,
    points_earned_ytd,
    points_redeemed_ytd,
    tier_start_date,
    lifetime_value
)
VALUES
    ('TEST001', 'Platinum', 15420, 8450, 3000, DATE '2023-01-01', 20000),
    ('TEST002', 'Silver', 3250, 8920, 2000, DATE '2023-03-15', 54000),
    ('TEST003', 'Gold', 8750, 19340, 0, DATE '2022-11-20', 8900),
    ('TEST004', 'Bronze', 1200, 3450, 0, DATE '2023-08-10', 3700)
 ;