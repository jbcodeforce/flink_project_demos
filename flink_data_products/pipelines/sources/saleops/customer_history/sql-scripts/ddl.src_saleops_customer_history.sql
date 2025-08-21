CREATE TABLE IF NOT EXISTS src_saleops_customer_history (
  -- Natural Key
  customer_id STRING NOT NULL,                -- Business key from source system
  
  -- Customer Personal Information
  first_name STRING,                          -- Customer first name
  last_name STRING,                           -- Customer last name
  company_name STRING,                        -- Company name (for business customers)
  email STRING,                               -- Customer email address
  phone STRING,                               -- Customer phone number
  
  -- Address Information
  address_line1 STRING,                       -- Street address
  address_line2 STRING,                       -- Apartment/suite number
  city STRING,                                -- City
  state_province STRING,                      -- State or province
  postal_code STRING,                         -- ZIP/postal code
  country STRING,                             -- Country
  
  -- Customer Lifecycle
  registration_date DATE,                     -- When customer first registered
  last_activity_date DATE,                    -- Last activity date
  lifetime_value DECIMAL(15,2),               -- Customer lifetime value
  preferred_language STRING,                  -- Customer's preferred language
  
  -- SCD Type 2 Change Tracking
  effective_start_date DATE NOT NULL,         -- When this version became effective
  effective_end_date DATE,                    -- When this version expired (NULL for current)
  change_type STRING,                         -- INSERT, UPDATE, DELETE
  
  -- Source System Tracking
  source_system STRING,                       -- System where data originated
  source_timestamp TIMESTAMP(3),              -- When data was captured from source
  
  -- Deduplication Fields
  record_hash STRING,                         -- Hash of all non-audit columns for change detection
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(customer_id, effective_start_date) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 3 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);