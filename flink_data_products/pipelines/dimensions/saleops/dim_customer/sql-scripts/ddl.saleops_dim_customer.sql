CREATE TABLE IF NOT EXISTS saleops_dim_customer (
  -- Surrogate Key
  customer_key BIGINT NOT NULL,               -- Auto-generated surrogate key
  
  -- Natural Key 
  customer_id STRING NOT NULL,                -- Business key from source system
  
  -- Customer Attributes
  customer_name STRING NOT NULL,              -- Full customer name
  email STRING,                               -- Customer email address
  phone STRING,                               -- Customer phone number
  
  -- Address Information
  address_line1 STRING,                       -- Street address
  address_line2 STRING,                       -- Apartment/suite number
  city STRING,                                -- City
  state_province STRING,                      -- State or province
  postal_code STRING,                         -- ZIP/postal code
  country STRING,                             -- Country
  
  -- Customer Segmentation
  customer_type STRING,                       -- Individual, Business, VIP, etc.
  customer_segment STRING,                    -- High Value, Standard, New, etc.
  preferred_language STRING,                  -- Customer's preferred language
  
  -- SCD Type 2 Fields
  effective_start_date DATE NOT NULL,         -- When this version became effective
  effective_end_date DATE NOT NULL,           -- When this version expired (9999-12-31 for current)
  is_current BOOLEAN NOT NULL DEFAULT TRUE,   -- Flag for current version
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(customer_key) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_key) INTO 3 BUCKETS
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