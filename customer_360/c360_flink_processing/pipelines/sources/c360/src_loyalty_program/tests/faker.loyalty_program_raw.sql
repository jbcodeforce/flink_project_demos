set 'client.statement-name' = 'insert-app-usage-raw';
create table loyalty_program_raw (
    customer_id STRING,
    loyalty_tier STRING,
    points_balance INTEGER,
    points_earned_ytd INTEGER,
    points_redeemed_ytd INTEGER,
    tier_start_date DATE,
    lifetime_value DECIMAL(10,2)
) distributed by hash(customer_id) into 1 buckets with (
    'changelog.mode' = 'append',
   'connector' = 'faker',
   'number-of-rows' = '500',
   'changelog.mode' = 'append',
   'fields.customer_id.expression' = '#{numerify ''CUST###''}',
   'fields.loyalty_tier.expression' = '#{Options.option ''Gold'',''Silver'',''Bronze''}',
   'fields.points_balance.expression' = '#{Number.numberBetween ''1000'',''10000''}',
   'fields.points_earned_ytd.expression' = '#{Number.numberBetween ''1000'',''10000''}',
   'fields.points_redeemed_ytd.expression' = '#{Number.numberBetween ''1000'',''10000''}',
   'fields.tier_start_date.expression' = '#{date.past ''30'', ''DAYS''}',
   'fields.lifetime_value.expression' = '#{Number.numberBetween ''1000'',''10000''}'
)