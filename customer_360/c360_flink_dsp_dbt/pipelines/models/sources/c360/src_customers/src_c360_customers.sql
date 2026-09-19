{{ config(
    materialized='streaming_table',
    distributed_by='customer_id',
    with={
        'changelog.mode': 'upsert',
        'key.avro-registry.schema-context': '.flink-dev',
        'value.avro-registry.schema-context': '.flink-dev',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'kafka.retention.time': '0',
        'kafka.producer.compression.type': 'snappy',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.fields-include': 'all'
    }
) }}

-- Migrated from dml.src_c360_customers.sql
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
        country,
        event_ts
    FROM (
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
            `$rowtime` as event_ts, -- propagate src ts to downstream
            ROW_NUMBER() OVER (
                PARTITION BY customer_id 
                ORDER BY `$rowtime` DESC
            ) AS row_num
        FROM {{ source('sql_scripts', 'customers_raw') }}
        WHERE customer_id IS NOT NULL 
    ) WHERE row_num = 1
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
    TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts) age_years,
    TIMESTAMPDIFF(DAY, CAST(registration_date AS TIMESTAMP_LTZ(3)), event_ts) as days_since_registration,
     CASE
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts)  < 25 THEN 'Gen Z'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts)  < 40 THEN 'Millennial'
        WHEN TIMESTAMPDIFF(YEAR, CAST(date_of_birth AS TIMESTAMP_LTZ(3)), event_ts) < 55 THEN 'Gen X'
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
