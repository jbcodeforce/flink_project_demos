CREATE TABLE IF NOT EXISTS c360_fct_customer_profile (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    customer_segment STRING,
    preferred_channel STRING,
    generation_segment STRING,
    age_years INTEGER,
    days_since_registration INTEGER,
    city STRING,
    state STRING,
    country STRING,
    
    -- Loyalty program data
    loyalty_tier STRING,
    points_balance INTEGER,
    lifetime_value DECIMAL(10,2),
    tier_rank INTEGER,
    value_segment STRING,
    redemption_rate DECIMAL(10,2),
    days_in_current_tier INTEGER,
    -- Transaction metrics
    total_transactions INTEGER,
    total_spent DECIMAL(10,2),
    avg_order_value DECIMAL(10,2),
    first_purchase_date TIMESTAMP(3),
    last_purchase_date TIMESTAMP(3),
    channels_used INTEGER,
    shopping_days INTEGER,
    preferred_channel_usage INTEGER,
    transactions_last_90d INTEGER,
    spent_last_90d DECIMAL(10,2),
    -- Support metrics
    total_support_tickets INTEGER,
    resolved_support_tickets INTEGER,
    avg_satisfaction DECIMAL(10,2),
    last_ticket_date TIMESTAMP(3),
    urgent_support_tickets INTEGER,
    -- App usage metrics
    total_app_sessions INTEGER,
    total_session_minutes INTEGER,
    avg_session_duration INTEGER,
    total_pages_viewed INTEGER,
    total_actions_taken INTEGER,
    last_app_use_date TIMESTAMP(3),
    unique_device_types INTEGER,
    app_engagement_score DECIMAL(10,2),
     -- Calculated customer health metrics
    customer_status STRING, 
     -- RFM-like scoring
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    profile_created_at TIMESTAMP(3),
  PRIMARY KEY(customer_id) NOT ENFORCED
) DISTRIBUTED BY HASH(customer_id) INTO 1 BUCKETS
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