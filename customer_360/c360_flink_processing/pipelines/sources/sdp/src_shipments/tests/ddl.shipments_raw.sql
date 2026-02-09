create table shipments_raw (
    shipment_id STRING,
    transaction_id STRING,
    tracking_number STRING,
    carrier STRING,
    service_level STRING,
    origin_location STRING,
    destination_address ROW<street STRING, city STRING, zipcode STRING, state STRING>,
    weight_kg DECIMAL(8,3),
    dimensions STRING,
    target_ship_date DATE,
    shipment_status STRING,
    shipping_cost DECIMAL(8,2)
) distributed by hash(shipment_id) into 1 buckets with (
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
