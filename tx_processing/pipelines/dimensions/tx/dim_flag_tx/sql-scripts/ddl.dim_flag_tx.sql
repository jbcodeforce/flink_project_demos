CREATE TABLE IF NOT EXISTS dim_flag_tx (
  `account_number` VARCHAR(255),
  `withdraw_total` DECIMAL(10, 2),
  `withdraw_count` INT,
  `merchant` VARCHAR(255),
  `flag` BOOLEAN,
  `reason` VARCHAR(255),
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE,
  WATERMARK FOR `timestamp` AS `timestamp` - INTERVAL '5' SECONDS,
  PRIMARY KEY(account_number) NOT ENFORCED
) DISTRIBUTED BY HASH(account_number) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'append',
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