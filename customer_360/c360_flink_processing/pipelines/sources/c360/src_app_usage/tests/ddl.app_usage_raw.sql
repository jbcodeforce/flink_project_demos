create table app_usage_raw (
    usage_id STRING,
    customer_id STRING,
    session_date DATE,
    session_start TIMESTAMP(3),
    session_duration_minutes INTEGER,
    pages_viewed INTEGER,
    actions_taken INTEGER,
    device_type STRING,
    app_version STRING
) distributed by hash(usage_id) into 1 buckets with (
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
)