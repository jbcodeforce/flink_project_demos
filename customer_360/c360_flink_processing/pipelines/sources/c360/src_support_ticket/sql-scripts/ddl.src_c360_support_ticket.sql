CREATE TABLE IF NOT EXISTS src_c360_support_ticket (
    ticket_id STRING,
    customer_id STRING,
    created_date TIMESTAMP(3),
    resolved_date TIMESTAMP(3),
    category STRING,
    priority STRING,
    status STRING,
    channel STRING,
    satisfaction_score INTEGER,
  -- put here column definitions
  PRIMARY KEY(ticket_id) NOT ENFORCED
) DISTRIBUTED BY HASH(ticket_id) INTO 1 BUCKETS
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