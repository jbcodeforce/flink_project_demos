CREATE TABLE IF NOT EXISTS src_tx_discounts (
  `city` VARCHAR(2147483647),
  `merchant_name` VARCHAR(2147483647),
  `min_transaction_value` DECIMAL(10, 2),
  `discount_amount` DECIMAL(10, 2),
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECONDS
) WITH (
  'changelog.mode' = 'append',
  'value.avro-registry.schema-context' = '.flink-dev',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);