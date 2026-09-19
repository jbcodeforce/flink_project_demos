{{ config(
    materialized='streaming_table',
    distributed_by='shipment_id',
    with={
        'changelog.mode': 'upsert',
        'key.avro-registry.schema-context': '.flink-dev',
        'value.avro-registry.schema-context': '.flink-dev',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'kafka.retention.time': '0',
        'kafka.producer.compression.type': 'snappy',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.fields-include': 'all'
    }
) }}

-- Migrated from dml.int_sdp_order_fulfillment.sql
-- Dimension: order fulfillment (shipment + order). One row per shipment.
-- Enriches shipment with transaction (customer_id, order amount, channel) for downstream fact.
SELECT
    s.shipment_id,
    s.transaction_id,
    t.customer_id,
    t.transaction_date,
    t.channel,
    t.channel_group,
    t.total_amount,
    s.tracking_number,
    s.carrier,
    s.service_level,
    s.origin_location,
    s.destination_address,
    s.weight_kg,
    s.dimensions,
    s.ship_date,
    s.estimated_delivery,
    s.actual_delivery,
    s.delivery_status,
    s.shipping_cost
FROM {{ ref('src_sdp_shipments') }} s
LEFT JOIN {{ ref('src_c360_transactions') }} t ON s.transaction_id = t.transaction_id
