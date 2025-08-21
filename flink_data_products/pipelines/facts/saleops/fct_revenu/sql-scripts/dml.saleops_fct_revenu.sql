INSERT INTO saleops_fct_revenu (
  time_key,
  product_key, 
  customer_key,
  order_id,
  line_item_id,
  sales_channel,
  revenue_amount,
  gross_revenue_amount,
  discount_amount,
  quantity,
  unit_price,
  cost_amount,
  profit_amount,
  tax_amount,
  created_at,
  updated_at
)
SELECT 
  -- Dimension Key Lookups (join with dimension tables to get surrogate keys)
  COALESCE(dt.time_key, -1) AS time_key,                    -- Default to -1 for unknown dates
  COALESCE(dp.product_key, -1) AS product_key,              -- Default to -1 for unknown products  
  COALESCE(dc.customer_key, -1) AS customer_key,            -- Default to -1 for unknown customers
  
  -- Degenerate Dimensions (direct from source)
  s.order_id,
  s.line_item_id,
  COALESCE(s.sales_channel, 'UNKNOWN') AS sales_channel,
  
  -- Calculated Measures
  s.unit_price * s.quantity - COALESCE(s.discount_amount, 0) AS revenue_amount,
  s.unit_price * s.quantity AS gross_revenue_amount,
  COALESCE(s.discount_amount, 0) AS discount_amount,
  s.quantity,
  s.unit_price,
  COALESCE(dp.standard_cost * s.quantity, 0) AS cost_amount,
  (s.unit_price * s.quantity - COALESCE(s.discount_amount, 0)) - 
    COALESCE(dp.standard_cost * s.quantity, 0) AS profit_amount,
  COALESCE(s.tax_amount, 0) AS tax_amount,
  
  -- Audit fields
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM src_saleops_sales_transactions s
  -- Join with Time Dimension 
  LEFT JOIN saleops_dim_time dt ON DATE_FORMAT(s.transaction_date, 'yyyyMMdd') = dt.date_key
  
  -- Join with Product Dimension
  LEFT JOIN saleops_dim_product dp ON s.product_id = dp.product_id 
    AND s.transaction_date >= dp.effective_start_date 
    AND s.transaction_date < dp.effective_end_date
  
  -- Join with Customer Dimension  
  LEFT JOIN saleops_dim_customer dc ON s.customer_id = dc.customer_id
    AND s.transaction_date >= dc.effective_start_date 
    AND s.transaction_date < dc.effective_end_date

WHERE 
  s.transaction_date >= CURRENT_DATE - INTERVAL '30' DAY  -- Process last 30 days
  AND s.status = 'COMPLETED'                              -- Only completed transactions
  AND s.quantity > 0                                      -- Valid quantity
  AND s.unit_price > 0                                    -- Valid price