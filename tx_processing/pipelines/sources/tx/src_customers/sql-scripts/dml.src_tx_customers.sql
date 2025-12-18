INSERT INTO src_tx_customers
SELECT 
  `account_number`,
  `customer_name`,
  `email`,
  `phone_number`,
  `date_of_birth`,
  `city`,
  `created_at`
from (
  select *,
  ROW_NUMBER() OVER ( PARTITION BY account_number ORDER BY `$rowtime` DESC) as row_num
  from customers_faker
) where row_num = 1;