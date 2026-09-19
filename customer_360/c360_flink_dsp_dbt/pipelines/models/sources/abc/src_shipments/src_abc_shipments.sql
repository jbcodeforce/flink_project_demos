{{ config(
    materialized='streaming_table',
    distributed_by='shipment_id',
    with={
        'changelog.mode': 'upsert',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'kafka.retention.time': '0',
        'kafka.producer.compression.type': 'snappy',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.fields-include': 'all'
    }
) }}

-- Migrated from dml.src_abc_shipments.sql
SELECT
    shipment_id,
    transaction_id,
    tracking_number,
    carrier,
    service_level,
    origin_location,
    destination_address.street AS street,
    destination_address.city AS city,
    destination_address.zipcode AS zipcode,
    destination_address.state AS state,
    weight_kg,
    dimensions,
    target_ship_date,
    shipment_status,
    shipping_cost
FROM {{ source('sql_scripts', 'shipments_raw') }} where shipment_status = 'delivered'
