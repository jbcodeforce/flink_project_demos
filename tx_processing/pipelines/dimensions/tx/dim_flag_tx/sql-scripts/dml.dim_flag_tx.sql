insert into dim_flag_tx
with flagged_above_total_withdraw as (
  SELECT 
    `account_number`,
    SUM(amount) OVER  w as withdraw_total,
     merchant,
     CASE WHEN SUM(amount) OVER  w  > '1000' 
          THEN TRUE
          ELSE FALSE END AS flag,
    'Over 1000 withdraw' as reason,
    `timestamp`
    FROM src_tx_transactions
    WHERE transaction_type = 'withdrawal'

WINDOW w AS (
 PARTITION BY account_number, transaction_type
    ORDER BY `timestamp` ASC
    ROWS BETWEEN 5 PRECEDING  AND CURRENT ROW)
) 

merchant_within_2_hours as (
  SELECT
    `account_number`,
    SUM(amount) OVER  w as withdraw_total,
    merchant,
     count(*) over w as withdraw_count,
    CASE WHEN COUNT(*) OVER  w  > 2
          THEN TRUE
          ELSE FALSE END AS flag,
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

select 
  
select * from flagged_above_total_withdraw;