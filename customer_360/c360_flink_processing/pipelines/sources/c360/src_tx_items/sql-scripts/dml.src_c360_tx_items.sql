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