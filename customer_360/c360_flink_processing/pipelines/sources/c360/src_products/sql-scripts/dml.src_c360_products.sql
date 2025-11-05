INSERT INTO src_c360_products
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
    TIMESTAMPDIFF(DAY, CAST(created_date AS TIMESTAMP(3)), CURRENT_DATE) as days_since_launch,
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