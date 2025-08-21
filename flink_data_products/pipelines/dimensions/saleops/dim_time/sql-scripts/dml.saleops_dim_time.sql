INSERT INTO saleops_dim_time (
  time_key,
  date_key,
  full_date,
  day_of_month,
  day_of_week,
  day_of_week_name,
  day_of_week_abbr,
  day_of_year,
  week_of_year,
  iso_week,
  week_start_date,
  week_end_date,
  month_number,
  month_name,
  month_abbr,
  month_start_date,
  month_end_date,
  quarter_number,
  quarter_name,
  quarter_start_date,
  quarter_end_date,
  year_number,
  fiscal_year,
  is_weekday,
  is_weekend,
  is_holiday,
  is_business_day,
  created_at,
  updated_at
)
SELECT 
  -- Generate surrogate key from date
  ABS(HASH_CODE(date_key)) AS time_key,
  
  -- Date keys and core date
  date_key,
  full_date,
  
  -- Date components
  EXTRACT(DAY FROM full_date) AS day_of_month,
  EXTRACT(DOW FROM full_date) + 1 AS day_of_week,  -- Adjust to Monday=1
  CASE EXTRACT(DOW FROM full_date) + 1
    WHEN 1 THEN 'Monday' WHEN 2 THEN 'Tuesday' WHEN 3 THEN 'Wednesday'
    WHEN 4 THEN 'Thursday' WHEN 5 THEN 'Friday' WHEN 6 THEN 'Saturday'
    WHEN 7 THEN 'Sunday'
  END AS day_of_week_name,
  CASE EXTRACT(DOW FROM full_date) + 1
    WHEN 1 THEN 'Mon' WHEN 2 THEN 'Tue' WHEN 3 THEN 'Wed'
    WHEN 4 THEN 'Thu' WHEN 5 THEN 'Fri' WHEN 6 THEN 'Sat'
    WHEN 7 THEN 'Sun'
  END AS day_of_week_abbr,
  EXTRACT(DOY FROM full_date) AS day_of_year,
  
  -- Week attributes
  EXTRACT(WEEK FROM full_date) AS week_of_year,
  CONCAT(CAST(EXTRACT(YEAR FROM full_date) AS STRING), '-W', 
         LPAD(CAST(EXTRACT(WEEK FROM full_date) AS STRING), 2, '0')) AS iso_week,
  full_date - INTERVAL (EXTRACT(DOW FROM full_date)) DAYS AS week_start_date,
  full_date + INTERVAL (6 - EXTRACT(DOW FROM full_date)) DAYS AS week_end_date,
  
  -- Month attributes
  EXTRACT(MONTH FROM full_date) AS month_number,
  CASE EXTRACT(MONTH FROM full_date)
    WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
    WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
    WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
    WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
  END AS month_name,
  CASE EXTRACT(MONTH FROM full_date)
    WHEN 1 THEN 'Jan' WHEN 2 THEN 'Feb' WHEN 3 THEN 'Mar'
    WHEN 4 THEN 'Apr' WHEN 5 THEN 'May' WHEN 6 THEN 'Jun'
    WHEN 7 THEN 'Jul' WHEN 8 THEN 'Aug' WHEN 9 THEN 'Sep'
    WHEN 10 THEN 'Oct' WHEN 11 THEN 'Nov' WHEN 12 THEN 'Dec'
  END AS month_abbr,
  DATE_TRUNC('MONTH', full_date) AS month_start_date,
  LAST_DAY(full_date) AS month_end_date,
  
  -- Quarter attributes
  EXTRACT(QUARTER FROM full_date) AS quarter_number,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM full_date) AS STRING)) AS quarter_name,
  DATE_TRUNC('QUARTER', full_date) AS quarter_start_date,
  LAST_DAY(DATE_TRUNC('QUARTER', full_date) + INTERVAL '2' MONTH) AS quarter_end_date,
  
  -- Year attributes
  EXTRACT(YEAR FROM full_date) AS year_number,
  -- Fiscal year (assuming fiscal year starts in April)
  CASE 
    WHEN EXTRACT(MONTH FROM full_date) >= 4 THEN EXTRACT(YEAR FROM full_date)
    ELSE EXTRACT(YEAR FROM full_date) - 1 
  END AS fiscal_year,
  
  -- Business calendar flags
  CASE WHEN EXTRACT(DOW FROM full_date) + 1 BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_weekday,
  CASE WHEN EXTRACT(DOW FROM full_date) + 1 IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  FALSE AS is_holiday,  -- Can be updated with holiday logic
  CASE WHEN EXTRACT(DOW FROM full_date) + 1 BETWEEN 1 AND 5 THEN TRUE ELSE FALSE END AS is_business_day,
  
  -- Audit fields
  CURRENT_TIMESTAMP AS created_at,
  CURRENT_TIMESTAMP AS updated_at

FROM (
  SELECT 
    DATE_FORMAT(date_seq, 'yyyyMMdd') AS date_key,
    date_seq AS full_date
  FROM UNNEST(
    SEQUENCE(
      DATE '2020-01-01',  -- Start date
      DATE '2030-12-31',  -- End date  
      INTERVAL '1' DAY
    )
  ) AS t(date_seq)
) dates