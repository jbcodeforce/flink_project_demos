CREATE TABLE IF NOT EXISTS src_tx_transactions (
 (PRIMARY KEY (txn_id) NOT ENFORCED,
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
)
AS 
SELECT * FROM `transactions_faker`;