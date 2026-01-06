
insert into src_tx_transactions 
select 
  `txn_id`,
  `account_number`,
  `timestamp`,
  `amount`,
  `currency`,
  `merchant`,
  `location`,
  `status`,
  `transaction_type`
from  transactions_faker;