CREATE TABLE IF NOT EXISTS src_c360_tx_items (
    item_id STRING,
    transaction_id STRING,
    product_id STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    discount_applied DECIMAL(10,2),
  PRIMARY KEY(item_id) NOT ENFORCED
) DISTRIBUTED BY HASH(item_id) INTO 1 BUCKETS
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