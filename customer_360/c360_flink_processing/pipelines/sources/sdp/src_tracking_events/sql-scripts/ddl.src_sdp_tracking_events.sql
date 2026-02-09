CREATE TABLE IF NOT EXISTS src_sdp_tracking_events (
    event_id STRING,
    tracking_number STRING,
    event_timestamp TIMESTAMP(3),
    event_type STRING,
    location STRING,
    description STRING,
    carrier_status STRING,
  PRIMARY KEY(event_id) NOT ENFORCED
) DISTRIBUTED BY HASH(event_id) INTO 1 BUCKETS
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
