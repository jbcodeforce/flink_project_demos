{{ config(
    materialized='streaming_table',
    distributed_by='event_id',
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

-- Migrated from dml.src_sdp_tracking_events.sql
SELECT
    event_id,
    shipment_id,
    event_timestamp,
    event_type,
    location,
    description,
    carrier_status
FROM {{ source('sql_scripts', 'tracking_events_raw') }}
where (carrier_status <> 'cancelled' or carrier_status <> 'delivered')
 and location is not null
