insert into dim_flag_tx
with flagged_above_withdraw_total as (
  SELECT 
    `account_number`,
    SUM(amount) OVER  w as withdraw_total,
     merchant,
     CASE WHEN SUM(amount) OVER  w  > '9900' 
          THEN TRUE
          ELSE FALSE END AS withdraw_total_flag,
    'Over 9900 withdraw' as reason,
    `timestamp`
    FROM src_tx_transactions
    WHERE transaction_type = 'withdrawal'

WINDOW w AS (
 PARTITION BY account_number, transaction_type
    ORDER BY `timestamp` ASC
    RANGE BETWEEN INTERVAL '5' HOUR PRECEDING AND CURRENT ROW)
) 

flagged_merchant_within_2_hours as (
  SELECT
    `account_number`,
    SUM(amount) OVER  w as withdraw_total,
    merchant,
     count(*) over w as withdraw_count,
    CASE WHEN COUNT(*) OVER  w  > 2
          THEN TRUE
          ELSE FALSE END AS merchant_within_2_hours_flag,
    'Multiple withdraws in 2 hours' as reason,
    `timestamp`
    FROM src_tx_transactions
    WHERE transaction_type = 'withdrawal'
    AND `timestamp` BETWEEN CURRENT_TIMESTAMP - INTERVAL '2' HOURS AND CURRENT_TIMESTAMP
    WINDOW w AS (
    PARTITION BY account_number, merchant
        ORDER BY `timestamp` ASC
        RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)
    
)

all_flagged as (  
  select * from flagged_above_withdraw_total where withdraw_total_flag = TRUE
  union all
  select * from flagged_merchant_within_2_hours where merchant_within_2_hours_flag = TRUE
)

select * from all_flagged;