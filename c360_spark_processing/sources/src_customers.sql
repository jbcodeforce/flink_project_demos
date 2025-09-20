-- Source: Customer base data from CRM system
-- Description: Core customer information including demographics and registration details
-- Data Owner: Customer Experience/CRM Team

-- Create temporary view for raw customer data
CREATE OR REPLACE TEMPORARY VIEW customers_raw
USING CSV
OPTIONS (
  path "../c360_mock_data/customer/customers.csv",
  header "true",
  inferSchema "true"
);

-- Create enriched customer view
CREATE OR REPLACE TEMPORARY VIEW src_customers AS
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    CAST(date_of_birth AS DATE) as date_of_birth,
    gender,
    CAST(registration_date AS TIMESTAMP) as registration_date,
    customer_segment,
    preferred_channel,
    address_line1,
    city,
    state,
    zip_code,
    country,
    -- Add derived fields
    DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 as age_years,
    DATEDIFF(CURRENT_DATE(), CAST(registration_date AS TIMESTAMP)) as days_since_registration,
    CASE 
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 25 THEN 'Gen Z'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 40 THEN 'Millennial'
        WHEN DATEDIFF(CURRENT_DATE(), CAST(date_of_birth AS DATE)) / 365.25 < 55 THEN 'Gen X'
        ELSE 'Boomer+'
    END as generation_segment,
    -- Data quality flags
    CASE WHEN email IS NULL OR email = '' THEN 1 ELSE 0 END as missing_email_flag,
    CASE WHEN phone IS NULL OR phone = '' THEN 1 ELSE 0 END as missing_phone_flag
FROM customers_raw;
