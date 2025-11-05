create table support_ticket_raw (
    ticket_id STRING,
    customer_id STRING,
    created_date TIMESTAMP(3),
    resolved_date TIMESTAMP(3),
    category STRING,
    priority STRING,
    status STRING,
    channel STRING,
    satisfaction_score INTEGER
) distributed by hash(ticket_id) into 1 buckets with (
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