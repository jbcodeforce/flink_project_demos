-- Deduplication: one row per item_id (latest by event time).
-- Source: transaction_items_raw (append-only; may contain duplicates).
-- Strategy: ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY $rowtime DESC) then keep row_num = 1.
-- Sink: src_c360_tx_items (upsert by item_id).
INSERT INTO src_c360_tx_items
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
    FROM transaction_items_raw
)
WHERE row_num = 1