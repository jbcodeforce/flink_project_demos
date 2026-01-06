SET 'sql.state-ttl' = '1 hour';
CREATE TABLE IF NOT EXISTS src_tx_transactions (
  `txn_id` VARCHAR(36) NOT NULL,
  `account_number` VARCHAR(255),
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE,
  `amount` DECIMAL(10, 2),
  `currency` VARCHAR(5),
  `merchant` VARCHAR(255),
  `location` VARCHAR(255),
  `status` VARCHAR(255),
  `transaction_type` VARCHAR(50),
  CONSTRAINT `PK_txn_id` PRIMARY KEY (`txn_id`) NOT ENFORCED,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECONDS)
WITH(
  'changelog.mode'= 'append',
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