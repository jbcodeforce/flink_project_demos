CREATE TABLE IF NOT EXISTS dim_sdp_order_fulfillment (
    shipment_id STRING,
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP(3),
    channel STRING,
    channel_group STRING,
    total_amount DECIMAL(10,2),
    tracking_number STRING,
    carrier STRING,
    service_level STRING,
    origin_location STRING,
    destination_address STRING,
    weight_kg DECIMAL(8,3),
    dimensions STRING,
    ship_date DATE,
    estimated_delivery DATE,
    actual_delivery DATE,
    delivery_status STRING,
    shipping_cost DECIMAL(8,2),
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
