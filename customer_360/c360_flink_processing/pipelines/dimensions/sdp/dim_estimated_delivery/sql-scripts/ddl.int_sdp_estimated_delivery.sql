-- Dimension: estimated delivery and time window per shipment.
-- Uses UDF estimate_delivery(event_timestamp, current_location, destination_address)
-- returning ROW<estimated_delivery_ts TIMESTAMP(3), window_start TIMESTAMP(3), window_end TIMESTAMP(3)>.
CREATE TABLE IF NOT EXISTS dim_sdp_estimated_delivery (
    shipment_id STRING,
    event_id STRING,
    event_timestamp TIMESTAMP(3),
    current_location STRING,
    estimated_delivery_ts TIMESTAMP(3),
    time_window_start TIMESTAMP(3),
    time_window_end TIMESTAMP(3),
  PRIMARY KEY(shipment_id) NOT ENFORCED
) DISTRIBUTED BY HASH(shipment_id) INTO 1 BUCKETS
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
