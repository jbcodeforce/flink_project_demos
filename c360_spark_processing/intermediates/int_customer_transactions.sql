-- Intermediate: Customer transaction history with enriched context
-- Description: Combines customer and transaction data for C360 analysis
-- Dependencies: src_customers, src_transactions, src_products, transaction_items

-- Create temporary view for transaction items data
CREATE OR REPLACE TEMPORARY VIEW transaction_items_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/sales/transaction_items.csv",
  header "true",
  inferSchema "true"
);

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
