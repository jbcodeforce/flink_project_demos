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

-- Migrated from dml.int_sdp_estimated_delivery.sql
-- Dimension: estimated delivery and time window. One row per shipment (latest tracking event).
-- Assumes UDF: estimate_delivery(event_timestamp, current_location, destination_address)
-- with destination_address as ROW<street STRING, city STRING, zipcode STRING, state STRING>
-- returning ROW<estimated_delivery_ts TIMESTAMP(3), window_start TIMESTAMP(3), window_end TIMESTAMP(3)>.
SELECT
    with_est.shipment_id,
    with_est.event_id,
    with_est.event_timestamp,
    with_est.current_location,
    with_est.est.estimated_delivery_ts,
    with_est.est.window_start AS time_window_start,
    with_est.est.window_end AS time_window_end
FROM (
    SELECT
        latest.shipment_id,
        latest.event_id,
        latest.event_timestamp,
        latest.location AS current_location,
        estimate_delivery(
            latest.event_timestamp,
            latest.location,
            ROW(latest.street, latest.city, latest.zipcode, latest.state)
        ) AS est
    FROM (
        SELECT
            e.shipment_id,
            e.event_id,
            e.event_timestamp,
            e.location,
            s.street,
            s.city,
            s.zipcode,
            s.state,
            ROW_NUMBER() OVER (
                PARTITION BY e.shipment_id
                ORDER BY e.event_timestamp DESC
            ) AS row_num
        FROM {{ ref('src_sdp_tracking_events') }} e
        INNER JOIN {{ ref('src_sdp_shipments') }} s ON e.shipment_id = s.shipment_id
    ) latest
    WHERE latest.row_num = 1
) AS with_est
