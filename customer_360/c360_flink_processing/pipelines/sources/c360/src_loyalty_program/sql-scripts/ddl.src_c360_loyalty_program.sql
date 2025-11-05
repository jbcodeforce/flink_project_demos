CREATE TABLE IF NOT EXISTS src_c360_loyalty_program (
    customer_id STRING,
    loyalty_tier STRING,
    points_balance INTEGER,
    points_earned_ytd INTEGER,
    points_redeemed_ytd INTEGER,
    tier_start_date DATE,
    lifetime_value DECIMAL(10,2),
    net_points_ytd INTEGER,
    redemption_rate DECIMAL(10,2),
    days_in_current_tier INTEGER,
    tier_rank INTEGER,
    value_segment STRING,
    -- put here column definitions
  PRIMARY KEY(customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
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