INSERT INTO src_c360_customers
with deduplicated_customers as (
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
        state,
        zip_code,
        country
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id 
                ORDER BY `$rowtime` DESC
            ) AS row_num
        FROM customers_raw
        WHERE customer_id IS NOT NULL 
    )
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
    state,
    zip_code,
    country,
    TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP(3)), CURRENT_DATE) age_years,
    TIMESTAMPDIFF(DAY, CAST(registration_date AS TIMESTAMP(3)), CURRENT_DATE) as days_since_registration,
     CASE
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP(3)), CURRENT_DATE)  < 25 THEN 'Gen Z'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP(3)), CURRENT_DATE)  < 40 THEN 'Millennial'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP(3)), CURRENT_DATE) < 55 THEN 'Gen X'
        ELSE 'Boomer+' END AS generation_segment,
    CASE
        WHEN email IS NULL
        OR email = '' THEN 1
        ELSE 0 END AS missing_email_flag,
     CASE
        WHEN phone IS NULL
        OR phone = '' THEN 1
        ELSE 0 END AS missing_phone_flag
FROM deduplicated_customers
    WHERE row_num = 1

