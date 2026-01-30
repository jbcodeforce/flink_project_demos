create table tracking_events_raw (
    event_id STRING,
    shipment_id STRING,
    event_timestamp TIMESTAMP(3),
    event_type STRING,
    location STRING,
    description STRING,
    carrier_status STRING
) distributed by hash(event_id) into 1 buckets with (
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
