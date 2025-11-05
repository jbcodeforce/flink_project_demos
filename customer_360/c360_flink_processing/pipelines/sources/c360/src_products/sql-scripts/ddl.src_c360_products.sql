CREATE TABLE IF NOT EXISTS src_c360_products (
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    weight_kg DECIMAL(10, 3),
    dimensions STRING,
    color STRING,
    size STRING,
    created_date DATE,
    status STRING,
    gross_margin DECIMAL(10, 2),
    margin_percent DECIMAL(10, 2),
    days_since_launch BIGINT,
    price_segment STRING,
    category_group STRING,
  -- put here column definitions
  PRIMARY KEY(product_id) NOT ENFORCED
) DISTRIBUTED BY HASH(product_id) INTO 1 BUCKETS
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