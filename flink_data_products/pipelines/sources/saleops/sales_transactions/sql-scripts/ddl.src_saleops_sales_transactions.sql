CREATE TABLE IF NOT EXISTS src_saleops_sales_transactions (
  -- Transaction Identifiers (Degenerate Dimension Keys)
  order_id STRING NOT NULL,                   -- Order identifier
  line_item_id STRING NOT NULL,              -- Line item within the order
  transaction_id STRING NOT NULL,             -- Unique transaction identifier
  
  -- Foreign Keys to Dimensions
  customer_id STRING NOT NULL,                -- Reference to customer dimension
  product_id STRING NOT NULL,                 -- Reference to product dimension
  
  -- Transaction Details
  transaction_date DATE NOT NULL,             -- Date of the transaction
  transaction_timestamp TIMESTAMP(3) NOT NULL, -- Exact timestamp of transaction
  sales_channel STRING,                       -- Channel: online, retail, wholesale, mobile
  
  -- Financial Measures
  unit_price DECIMAL(10,2) NOT NULL,         -- Unit price at time of sale
  quantity INT NOT NULL,                      -- Quantity of products sold
  discount_amount DECIMAL(15,2) DEFAULT 0.0, -- Applied discount amount
  tax_amount DECIMAL(15,2) DEFAULT 0.0,      -- Tax amount
  shipping_amount DECIMAL(15,2) DEFAULT 0.0, -- Shipping cost
  
  -- Calculated Amounts (for validation)
  gross_amount DECIMAL(15,2),                 -- unit_price * quantity
  net_amount DECIMAL(15,2),                   -- gross_amount - discount_amount
  total_amount DECIMAL(15,2),                 -- net_amount + tax_amount + shipping_amount
  
  -- Transaction Status and Processing
  status STRING NOT NULL,                     -- PENDING, COMPLETED, CANCELLED, REFUNDED
  payment_method STRING,                      -- Credit Card, PayPal, Cash, etc.
  currency_code STRING DEFAULT 'USD',        -- Currency code
  
  -- Source System Tracking
  source_system STRING,                       -- System where transaction originated
  source_timestamp TIMESTAMP(3),              -- When data was captured from source
  original_transaction_id STRING,             -- Original ID from source system
  
  -- Deduplication Fields
  record_hash STRING,                         -- Hash of all transaction attributes for change detection
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(order_id, line_item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id, product_id) INTO 6 BUCKETS
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