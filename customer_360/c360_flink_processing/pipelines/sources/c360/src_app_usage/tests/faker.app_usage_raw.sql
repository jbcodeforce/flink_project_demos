create table app_usage_raw (
    usage_id STRING,
    customer_id STRING,
    session_date DATE,
    session_start TIMESTAMP(3),
    session_duration_minutes INTEGER,
    pages_viewed INTEGER,
    actions_taken INTEGER,
    device_type STRING,
    app_version STRING
) distributed by hash(usage_id) into 1 buckets with (
    'connector' = 'faker',
    'rows-per-second' = '5',
    'changelog.mode' = 'append',
    'fields.usage_id.expression' = '#{IdNumber.valid}',
    'fields.customer_id.expression' = '#{numerify ''CUST###''}',
    'fields.session_date.expression' = '#{date.past ''30'', ''DAYS''}',
    'fields.session_start.expression' = '#{date.past ''30'', ''5'', ''SECONDS''}',
    'fields.session_duration_minutes.expression' = '#{Number.numberBetween ''10'',''60''}',
    'fields.pages_viewed.expression' = '#{Number.numberBetween ''10'',''100''}',
    'fields.actions_taken.expression' = '#{Number.numberBetween ''10'',''100''}',
    'fields.device_type.expression' = '#{Options.option ''ios'',''android'',''web''}',
    'fields.app_version.expression' = '#{Options.option ''1.0.0'',''1.0.1'',''1.0.2''}'
)