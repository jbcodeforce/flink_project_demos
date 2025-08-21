CREATE TABLE IF NOT EXISTS saleops_fct_revenu (
  -- Dimension Keys (Foreign Keys to Dimension Tables)
  time_key BIGINT NOT NULL,                    -- Reference to dim_time (date dimension)
  product_key BIGINT NOT NULL,                 -- Reference to dim_product 
  customer_key BIGINT NOT NULL,                -- Reference to dim_customer
  
  -- Degenerate Dimensions (Transaction-level attributes stored in fact)
  order_id STRING NOT NULL,                    -- Original order identifier
  line_item_id STRING NOT NULL,               -- Line item within the order
  sales_channel STRING,                       -- Channel: online, retail, wholesale
  
  -- Measures (Additive Facts)
  revenue_amount DECIMAL(15,2) NOT NULL,      -- Net revenue amount
  gross_revenue_amount DECIMAL(15,2),         -- Gross revenue before discounts
  discount_amount DECIMAL(15,2) DEFAULT 0.0,  -- Applied discount amount
  quantity INT NOT NULL,                      -- Quantity of products sold
  unit_price DECIMAL(10,2) NOT NULL,         -- Unit price at time of sale
  cost_amount DECIMAL(15,2),                 -- Cost of goods sold
  profit_amount DECIMAL(15,2),               -- Calculated profit (revenue - cost)
  tax_amount DECIMAL(15,2) DEFAULT 0.0,      -- Tax amount
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(time_key, product_key, customer_key, order_id, line_item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(time_key, product_key, customer_key) INTO 6 BUCKETS
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