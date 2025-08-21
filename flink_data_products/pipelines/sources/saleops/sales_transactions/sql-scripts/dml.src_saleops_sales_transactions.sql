INSERT INTO src_saleops_sales_transactions (
  order_id,
  line_item_id,
  transaction_id,
  customer_id,
  product_id,
  transaction_date,
  transaction_timestamp,
  sales_channel,
  unit_price,
  quantity,
  discount_amount,
  tax_amount,
  shipping_amount,
  gross_amount,
  net_amount,
  total_amount,
  status,
  payment_method,
  currency_code,
  source_system,
  source_timestamp,
  original_transaction_id,
  record_hash,
  created_at,
  updated_at
)
WITH deduplicated_transactions AS (
  SELECT 
    order_id,
    line_item_id,
    transaction_id,
    customer_id,
    product_id,
    transaction_date,
    transaction_timestamp,
    sales_channel,
    unit_price,
    quantity,
    discount_amount,
    tax_amount,
    shipping_amount,
    
    -- Calculate derived amounts
    unit_price * quantity AS gross_amount,
    (unit_price * quantity) - COALESCE(discount_amount, 0) AS net_amount,
    (unit_price * quantity) - COALESCE(discount_amount, 0) + 
      COALESCE(tax_amount, 0) + COALESCE(shipping_amount, 0) AS total_amount,
    
    status,
    payment_method,
    COALESCE(currency_code, 'USD') AS currency_code,
    source_system,
    source_timestamp,
    original_transaction_id,
    
    -- Create hash of all transaction attributes for change detection
    MD5(CONCAT(
      COALESCE(order_id, ''),
      COALESCE(line_item_id, ''),
      COALESCE(transaction_id, ''),
      COALESCE(customer_id, ''),
      COALESCE(product_id, ''),
      COALESCE(CAST(transaction_date AS STRING), ''),
      COALESCE(sales_channel, ''),
      COALESCE(CAST(unit_price AS STRING), ''),
      COALESCE(CAST(quantity AS STRING), ''),
      COALESCE(CAST(discount_amount AS STRING), ''),
      COALESCE(CAST(tax_amount AS STRING), ''),
      COALESCE(CAST(shipping_amount AS STRING), ''),
      COALESCE(status, ''),
      COALESCE(payment_method, ''),
      COALESCE(currency_code, '')
    )) AS record_hash,
    
    -- Deduplication: Keep latest record per transaction key
    ROW_NUMBER() OVER (
      PARTITION BY order_id, line_item_id, transaction_timestamp
      ORDER BY source_timestamp DESC, created_at DESC
    ) AS row_num

  FROM raw_sales_events r
  WHERE 
    r.order_id IS NOT NULL
    AND r.line_item_id IS NOT NULL
    AND r.customer_id IS NOT NULL
    AND r.product_id IS NOT NULL
    AND r.transaction_date IS NOT NULL
    AND r.unit_price > 0
    AND r.quantity > 0
    AND r.source_timestamp >= CURRENT_TIMESTAMP - INTERVAL '1' DAY  -- Process last 24 hours
)
SELECT 
  order_id,
  line_item_id,
  transaction_id,
  customer_id,
  product_id,
  transaction_date,
  transaction_timestamp,
  sales_channel,
  unit_price,
  quantity,
  discount_amount,
  tax_amount,
  shipping_amount,
  gross_amount,
  net_amount,
  total_amount,
  status,
  payment_method,
  currency_code,
  source_system,
  source_timestamp,
  original_transaction_id,
  record_hash,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM deduplicated_transactions
WHERE row_num = 1  -- Keep only the latest record for each transaction key