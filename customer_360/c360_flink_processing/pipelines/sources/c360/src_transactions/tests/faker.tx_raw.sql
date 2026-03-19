set 'client.statement-name' = 'insert-tx-raw';
create table tx_raw (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP(3),
    channel STRING,
    store_id STRING,
    payment_method STRING,
    subtotal DECIMAL(10,2),
    tax_amount DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    currency STRING,
    status STRING
) distributed by hash(transaction_id) into 1 buckets with ( 
    'changelog.mode' = 'append',
    'connector' = 'faker',
    'rows-per-second' = '1',
    'fields.transaction_id.expression' = '#{IdNumber.valid}',
    'fields.customer_id.expression' = '#{numerify ''CUST###''}',
    'fields.transaction_date.expression' = '#{date.past ''30'', ''DAYS''}',
    'fields.channel.expression' = '#{Options.option ''online'',''store'',''mobile''}',
    'fields.store_id.expression' = '#{numerify ''ST00###''}',
    'fields.payment_method.expression' = '#{Options.option ''credit_card'',''debit_card'',''cash'',''paypal'',''apple_pay'',''google_pay''}',
    'fields.subtotal.expression' = '#{Number.numberBetween ''10'',''1000''}',
    'fields.tax_amount.expression' = '#{Number.numberBetween ''1'',''100''}',
    'fields.discount_amount.expression' = '#{Number.numberBetween ''1'',''100''}',
    'fields.total_amount.expression' = '#{Number.numberBetween ''10'',''1000''}',
    'fields.currency.expression' = '#{Options.option ''USD'',''EUR'',''GBP'',''JPY'',''CAD'',''AUD'',''NZD'',''CHF'',''CNY'',''INR''}',
    'fields.status.expression' = '#{Options.option ''completed'',''pending'',''failed''}'
);