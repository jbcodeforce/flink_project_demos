CREATE TABLE product_raw (
    product_id STRING,
    product_name STRING,
    category STRING,
    subcategory STRING,
    brand STRING,
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2),
    weight_kg DECIMAL(10, 3),
    dimensions STRING,
    color STRING,
    size STRING,
    created_date DATE,
    status STRING
) distributed by hash(product_id) into 1 buckets with (
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