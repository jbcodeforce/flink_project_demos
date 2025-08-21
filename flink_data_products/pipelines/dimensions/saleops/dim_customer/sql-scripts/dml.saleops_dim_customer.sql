INSERT INTO saleops_dim_customer (
  customer_key,
  customer_id,
  customer_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_province,
  postal_code,
  country,
  customer_type,
  customer_segment,
  preferred_language,
  effective_start_date,
  effective_end_date,
  is_current,
  created_at,
  updated_at
)
SELECT 
  -- Generate surrogate key using hash of natural key + effective date
  ABS(HASH_CODE(CONCAT(c.customer_id, CAST(c.effective_start_date AS STRING)))) AS customer_key,
  
  -- Natural key and attributes
  c.customer_id,
  COALESCE(c.first_name || ' ' || c.last_name, c.company_name, 'Unknown') AS customer_name,
  c.email,
  c.phone,
  c.address_line1,
  c.address_line2,
  c.city,
  c.state_province,
  c.postal_code,
  COALESCE(c.country, 'Unknown') AS country,
  
  -- Derived attributes
  CASE 
    WHEN c.company_name IS NOT NULL THEN 'Business'
    WHEN c.registration_date >= CURRENT_DATE - INTERVAL '90' DAY THEN 'New'
    ELSE 'Individual'
  END AS customer_type,
  
  CASE 
    WHEN c.lifetime_value >= 10000 THEN 'High Value'
    WHEN c.lifetime_value >= 1000 THEN 'Standard'
    ELSE 'New'
  END AS customer_segment,
  
  COALESCE(c.preferred_language, 'en') AS preferred_language,
  
  -- SCD Type 2 fields
  c.effective_start_date,
  COALESCE(c.effective_end_date, DATE '9999-12-31') AS effective_end_date,
  CASE WHEN c.effective_end_date IS NULL THEN TRUE ELSE FALSE END AS is_current,
  
  -- Audit fields
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM customer_history c
WHERE 
  c.customer_id IS NOT NULL
  AND c.effective_start_date IS NOT NULL