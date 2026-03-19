CREATE TABLE `discounts_faker` (
  `city` VARCHAR(2147483647),
  `merchant_name` VARCHAR(2147483647),
  `min_transaction_value` DECIMAL(10, 2),
  `discount_amount` DECIMAL(10, 2),
  `timestamp` TIMESTAMP(3) WITH LOCAL TIME ZONE
)
DISTRIBUTED INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'faker',
  'fields.city.expression' = '#{Options.option ''New York'',''Los Angeles'',''Chicago'',''Charlotte '',''San Francisco'',''Indianapolis'',''Seattle'',''Denver'',''Washington'',''Boston'',''El Paso'',''Nashville'',''Detroit'',''Oklahoma City'',''Portland'',''Las Vegas'',''Memphis'',''Louisville'',''Baltimore''}',
  'fields.discount_amount.expression' = '#{NUMBER.numberBetween ''2'',''20''}',
  'fields.merchant_name.expression' = '#{Options.option ''Walmart Inc.'', ''Amazon.com Inc.'', ''CVS Health'', ''Costco Wholesale Corporation'', ''Schwarz Group'', ''McKesson Corporation'', ''McDonalds Corporation'', ''Starbucks Corporation'', ''Cencora'', ''The Home Depot Inc.'', ''Yum! Brands'', ''The Kroger Co.'', ''Aldi Group'', ''Walgreens Boots Alliance'', ''Cardinal Health'', ''Subway'', ''JD.com Inc.'', ''Target Corporation'', ''Ahold Delhaize'', ''Lowe Companies Inc.''}',
  'fields.min_transaction_value.expression' = '#{NUMBER.numberBetween ''500'',''1000''}',
  'rows-per-second' = '5'
)
