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

-- Migrated from dml.sdp_fulfillment_analytics.sql
-- View: Fulfillment Analytics sdp Data Product
-- Description: Consumable view for order fulfillment and delivery experience analytics
-- Purpose: Real-time fulfillment health, SLA, carrier performance for ops and CX
-- Data Owner: Operations / Logistics
-- Dependencies: sdp_fct_order_fulfillment
SELECT
    shipment_id,
    transaction_id,
    customer_id,
    transaction_date,
    channel,
    channel_group,
    total_amount,
    tracking_number,
    carrier,
    service_level,
    origin_location,
    destination_address as destination_region,
    ship_date,
    estimated_delivery,
    actual_delivery,
    delivery_status,
    shipping_cost,
    weight_kg,
    sla_met,
    delay_days,
    CASE
        WHEN delivery_status <> 'delivered' THEN 'In Transit'
        WHEN sla_met = 1 THEN 'On Time'
        WHEN sla_met = 0 AND delay_days > 0 THEN 'Late'
        ELSE 'Unknown'
    END as sla_status,
    customer_segment,
    city,
    state,
    country,
    snapshot_at,
    `$rowtime` as view_created_at
FROM {{ ref('sdp_fct_order_fulfillment') }}
WHERE shipment_id IS NOT NULL
