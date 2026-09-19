{{ config(
    materialized='streaming_table',
    distributed_by='item_id',
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

-- Migrated from dml.src_c360_tx_items.sql
-- Deduplication: one row per item_id (latest by event time).
-- Source: transaction_items_raw (append-only; may contain duplicates).
-- Strategy: ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY $rowtime DESC) then keep row_num = 1.
-- Sink: src_c360_tx_items (upsert by item_id).
SELECT 
    item_id,
    transaction_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    discount_applied
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY item_id 
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM {{ source('sql_scripts', 'transaction_items_raw') }}
)
WHERE row_num = 1
