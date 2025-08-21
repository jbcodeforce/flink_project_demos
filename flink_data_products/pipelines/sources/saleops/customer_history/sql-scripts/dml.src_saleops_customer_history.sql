INSERT INTO src_saleops_customer_history (
  customer_id,
  first_name,
  last_name,
  company_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_province,
  postal_code,
  country,
  registration_date,
  last_activity_date,
  lifetime_value,
  preferred_language,
  effective_start_date,
  effective_end_date,
  change_type,
  source_system,
  source_timestamp,
  record_hash,
  created_at,
  updated_at
)
WITH deduplicated_customers AS (
  SELECT 
    customer_id,
    first_name,
    last_name,
    company_name,
    email,
    phone,
    address_line1,
    address_line2,
    city,
    state_province,
    postal_code,
    country,
    registration_date,
    last_activity_date,
    lifetime_value,
    preferred_language,
    effective_start_date,
    effective_end_date,
    change_type,
    source_system,
    source_timestamp,
    
    -- Create hash of all business attributes for change detection
    MD5(CONCAT(
      COALESCE(customer_id, ''),
      COALESCE(first_name, ''),
      COALESCE(last_name, ''), 
      COALESCE(company_name, ''),
      COALESCE(email, ''),
      COALESCE(phone, ''),
      COALESCE(address_line1, ''),
      COALESCE(address_line2, ''),
      COALESCE(city, ''),
      COALESCE(state_province, ''),
      COALESCE(postal_code, ''),
      COALESCE(country, ''),
      COALESCE(CAST(lifetime_value AS STRING), ''),
      COALESCE(preferred_language, '')
    )) AS record_hash,
    
    -- Deduplication: Keep latest record per customer_id and effective_start_date
    ROW_NUMBER() OVER (
      PARTITION BY customer_id, effective_start_date 
      ORDER BY source_timestamp DESC, created_at DESC
    ) AS row_num

  FROM raw_customer_changes r
  WHERE 
    r.customer_id IS NOT NULL
    AND r.effective_start_date IS NOT NULL
    AND r.source_timestamp >= CURRENT_TIMESTAMP - INTERVAL '7' DAY  -- Process last 7 days
)
SELECT 
  customer_id,
  first_name,
  last_name,
  company_name,
  email,
  phone,
  address_line1,
  address_line2,
  city,
  state_province,
  postal_code,
  country,
  registration_date,
  last_activity_date,
  lifetime_value,
  preferred_language,
  effective_start_date,
  effective_end_date,
  change_type,
  source_system,
  source_timestamp,
  record_hash,
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM deduplicated_customers
WHERE row_num = 1  -- Keep only the latest record for each customer_id + effective_start_date