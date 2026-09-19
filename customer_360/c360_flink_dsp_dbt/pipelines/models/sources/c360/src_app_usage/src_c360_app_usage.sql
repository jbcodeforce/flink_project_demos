{{ config(
    materialized='streaming_table',
    distributed_by='usage_id',
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

-- Migrated from dml.src_c360_app_usage.sql
SELECT 
    usage_id,
    customer_id,
    session_date,
    session_start,
    session_duration_minutes,
    pages_viewed,
    actions_taken,
    device_type,
    app_version
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM {{ source('sql_scripts', 'app_usage_raw') }}
)
WHERE row_num = 1
