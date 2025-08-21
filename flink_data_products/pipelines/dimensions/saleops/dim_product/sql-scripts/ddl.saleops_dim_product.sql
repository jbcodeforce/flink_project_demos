CREATE TABLE IF NOT EXISTS saleops_dim_product (
  -- Surrogate Key
  product_key BIGINT NOT NULL,                -- Auto-generated surrogate key
  
  -- Natural Key
  product_id STRING NOT NULL,                 -- Business key from source system
  
  -- Product Attributes
  product_name STRING NOT NULL,               -- Product display name
  product_description STRING,                 -- Detailed product description
  sku STRING,                                 -- Stock keeping unit
  upc STRING,                                 -- Universal product code
  
  -- Product Classification
  category STRING,                            -- Product category (Electronics, Clothing, etc.)
  subcategory STRING,                         -- Product subcategory 
  brand STRING,                               -- Product brand
  manufacturer STRING,                        -- Product manufacturer
  
  -- Product Metrics
  standard_cost DECIMAL(10,2),               -- Standard cost for profit calculations
  list_price DECIMAL(10,2),                  -- Manufacturer suggested retail price
  weight_kg DECIMAL(8,3),                    -- Product weight in kilograms
  
  -- Product Status
  status STRING,                              -- Active, Discontinued, Seasonal
  is_active BOOLEAN DEFAULT TRUE,             -- Whether product is currently active
  
  -- SCD Type 2 Fields  
  effective_start_date DATE NOT NULL,         -- When this version became effective
  effective_end_date DATE NOT NULL,           -- When this version expired (9999-12-31 for current)
  is_current BOOLEAN NOT NULL DEFAULT TRUE,   -- Flag for current version
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(product_key) NOT ENFORCED
) DISTRIBUTED BY HASH(product_key) INTO 3 BUCKETS
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