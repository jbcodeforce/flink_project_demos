CREATE TABLE IF NOT EXISTS src_sdp_shipments (
    shipment_id STRING,
    transaction_id STRING,
    tracking_number STRING,
    carrier STRING,
    service_level STRING,
    origin_location STRING,
    street STRING,
    city STRING,
    zipcode STRING,
    state STRING,
    weight_kg DECIMAL(8,3),
    dimensions STRING,
    target_ship_date DATE,
    shipment_status STRING,
    shipping_cost DECIMAL(8,2),
  PRIMARY KEY(shipment_id) NOT ENFORCED
) DISTRIBUTED BY HASH(shipment_id) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);
