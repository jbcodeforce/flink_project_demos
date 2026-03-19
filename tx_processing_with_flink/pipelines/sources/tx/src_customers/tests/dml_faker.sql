CREATE TABLE `customers_faker` (
  `account_number` VARCHAR(2147483647) NOT NULL,
  `customer_name` VARCHAR(2147483647),
  `email` VARCHAR(2147483647),
  `phone_number` VARCHAR(2147483647),
  `date_of_birth` TIMESTAMP(3),
  `city` VARCHAR(2147483647),
  `created_at` TIMESTAMP(3) WITH LOCAL TIME ZONE
)
DISTRIBUTED INTO 6 BUCKETS
WITH (
  'changelog.mode' = 'append',
  'connector' = 'faker',
  'fields.account_number.expression' = 'ACC#{Number.numberBetween ''1000000'',''1005000''}',
  'fields.city.expression' = '#{Address.city}',
   'fields.customer_name.expression' = '#{Name.fullName}',
  'fields.date_of_birth.expression' = '#{date.birthday ''18'',''50''}',
  'fields.phone_number.expression' = '#{PhoneNumber.cellPhone}',
  'fields.email.expression' = '#{Internet.emailAddress}',
  'rows-per-second' = '10'
)
