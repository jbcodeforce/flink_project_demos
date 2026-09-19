{{ config(
    materialized='streaming_table',
    distributed_by='ticket_id',
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

-- Migrated from dml.src_c360_support_ticket.sql
SELECT 
    ticket_id,
    customer_id,
    created_date,
    resolved_date,
    category,
    priority,
    status,
    channel,
    satisfaction_score
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id 
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM {{ source('sql_scripts', 'support_ticket_raw') }}
)
WHERE row_num = 1
