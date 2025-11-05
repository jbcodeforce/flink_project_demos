create table loyalty_program_raw (
    customer_id STRING,
    loyalty_tier STRING,
    points_balance INTEGER,
    points_earned_ytd INTEGER,
    points_redeemed_ytd INTEGER,
    tier_start_date DATE,
    lifetime_value DECIMAL(10,2)
) distributed by hash(customer_id) into 1 buckets with (
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