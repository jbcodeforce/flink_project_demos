INSERT INTO saleops_dim_product (
  product_key,
  product_id,
  product_name,
  product_description,
  sku,
  upc,
  category,
  subcategory,
  brand,
  manufacturer,
  standard_cost,
  list_price,
  weight_kg,
  status,
  is_active,
  effective_start_date,
  effective_end_date,
  is_current,
  created_at,
  updated_at
)
SELECT 
  -- Generate surrogate key using hash of natural key + effective date
  ABS(HASH_CODE(CONCAT(p.product_id, CAST(p.effective_start_date AS STRING)))) AS product_key,
  
  -- Natural key and core attributes
  p.product_id,
  p.product_name,
  p.product_description,
  p.sku,
  p.upc,
  
  -- Classification hierarchy
  COALESCE(p.category, 'Uncategorized') AS category,
  COALESCE(p.subcategory, 'Other') AS subcategory,
  COALESCE(p.brand, 'Generic') AS brand,
  COALESCE(p.manufacturer, 'Unknown') AS manufacturer,
  
  -- Financial and physical attributes
  p.standard_cost,
  p.list_price,
  p.weight_kg,
  
  -- Status fields
  COALESCE(p.status, 'Active') AS status,
  CASE WHEN COALESCE(p.status, 'Active') = 'Active' THEN TRUE ELSE FALSE END AS is_active,
  
  -- SCD Type 2 fields
  p.effective_start_date,
  COALESCE(p.effective_end_date, DATE '9999-12-31') AS effective_end_date,
  CASE WHEN p.effective_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current,
  
  -- Audit fields
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM product_history p
WHERE 
  p.product_id IS NOT NULL
  AND p.product_name IS NOT NULL
  AND p.effective_start_date IS NOT NULL