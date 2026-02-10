create table transaction_items_raw (
    item_id STRING,
    transaction_id STRING,
    product_id STRING,
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    line_total DECIMAL(10,2),
    discount_applied DECIMAL(10,2)
) distributed by hash(item_id) into 1 buckets with (
    'changelog.mode' = 'append',
    'connector' = 'faker',
    'rows-per-second' = '2',
    'fields.item_id.expression' = '#{IdNumber.valid}',
    'fields.transaction_id.expression' = '#{IdNumber.valid}',
    'fields.product_id.expression' = '#{IdNumber.valid}',
    'fields.quantity.expression' = '#{Number.numberBetween ''1'',''10''}',
    'fields.unit_price.expression' = '#{Number.numberBetween ''1'',''100''}',
    'fields.line_total.expression' = '#{Number.numberBetween ''1'',''1000''}',
    'fields.discount_applied.expression' = '#{Number.numberBetween ''1'',''100''}'
);