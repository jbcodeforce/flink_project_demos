-- Source: Customer base data from CRM system
-- Description: Core customer information including demographics and registration details
-- Data Owner: Customer Experience/CRM Team
-- Create temporary view for raw customer data
CREATE
OR REPLACE TEMPORARY VIEW customers_raw USING CSV OPTIONS (
  PATH "../ c360_mock_data / customer / customers.csv ",
  header " TRUE ",
  inferSchema " TRUE "
);

-- Create enriched customer view with deduplication
CREATE
OR REPLACE TEMPORARY VIEW src_customers AS WITH ranked_customers AS (
  SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    STATE,
    zip_code,
    country,
    -- Deduplication logic: prefer most recent + most complete records
    ROW_NUMBER() OVER (
      PARTITION BY customer_id
      ORDER
        BY CAST(registration_date AS TIMESTAMP) DESC -- Most recent registration
    ) AS row_num
  FROM
    customers_raw
  WHERE
    customer_id IS NOT NULL -- Filter out invalid records
)
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  date_of_birth,
  gender,
  registration_date,
  customer_segment,
  preferred_channel,
  address_line1,
  city,
  STATE,
  zip_code,
  country,
  -- Add derived fields
  DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25 AS age_years,
  DATEDIFF(CURRENT_DATE(), registration_date) AS days_since_registration,
  CASE
  WHEN DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25 < 25 THEN 'Gen Z'
  WHEN DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25 < 40 THEN 'Millennial'
  WHEN DATEDIFF(CURRENT_DATE(), date_of_birth) / 365.25 < 55 THEN 'Gen X'
  ELSE 'Boomer+' END AS generation_segment,
  -- Data quality flags
  CASE
  WHEN email IS NULL
  OR email = '' THEN 1
  ELSE 0 END AS missing_email_flag,
  CASE
  WHEN phone IS NULL
  OR phone = '' THEN 1
  ELSE 0 END AS missing_phone_flag,
  data_quality_score
FROM
  ranked_customers
WHERE
  row_num = 1;

-- Keep only the best record per customer_id