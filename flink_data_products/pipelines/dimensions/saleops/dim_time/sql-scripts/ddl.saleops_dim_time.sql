CREATE TABLE IF NOT EXISTS saleops_dim_time (
  -- Surrogate Key
  time_key BIGINT NOT NULL,                   -- Auto-generated surrogate key
  
  -- Natural Key
  date_key STRING NOT NULL,                   -- YYYYMMDD format (e.g., '20240315')
  full_date DATE NOT NULL,                    -- Actual date value
  
  -- Date Attributes
  day_of_month INT NOT NULL,                  -- 1-31
  day_of_week INT NOT NULL,                   -- 1-7 (Monday=1)
  day_of_week_name STRING NOT NULL,           -- Monday, Tuesday, etc.
  day_of_week_abbr STRING NOT NULL,           -- Mon, Tue, etc.
  day_of_year INT NOT NULL,                   -- 1-366
  
  -- Week Attributes
  week_of_year INT NOT NULL,                  -- 1-53
  iso_week STRING NOT NULL,                   -- ISO week format (YYYY-W##)
  week_start_date DATE NOT NULL,              -- Monday of the week
  week_end_date DATE NOT NULL,                -- Sunday of the week
  
  -- Month Attributes  
  month_number INT NOT NULL,                  -- 1-12
  month_name STRING NOT NULL,                 -- January, February, etc.
  month_abbr STRING NOT NULL,                 -- Jan, Feb, etc.
  month_start_date DATE NOT NULL,             -- First day of month
  month_end_date DATE NOT NULL,               -- Last day of month
  
  -- Quarter Attributes
  quarter_number INT NOT NULL,                -- 1-4
  quarter_name STRING NOT NULL,               -- Q1, Q2, Q3, Q4
  quarter_start_date DATE NOT NULL,           -- First day of quarter
  quarter_end_date DATE NOT NULL,             -- Last day of quarter
  
  -- Year Attributes
  year_number INT NOT NULL,                   -- 2024, 2025, etc.
  fiscal_year INT,                            -- Fiscal year (may differ from calendar year)
  
  -- Business Calendar Flags
  is_weekday BOOLEAN NOT NULL,                -- Monday-Friday
  is_weekend BOOLEAN NOT NULL,                -- Saturday-Sunday
  is_holiday BOOLEAN DEFAULT FALSE,           -- National/company holidays
  is_business_day BOOLEAN NOT NULL,           -- Weekday and not holiday
  
  -- Audit Fields
  created_at TIMESTAMP(3) NOT NULL,           -- When record was created
  updated_at TIMESTAMP(3) NOT NULL,           -- When record was last updated
  
  PRIMARY KEY(time_key) NOT ENFORCED
) DISTRIBUTED BY HASH(time_key) INTO 1 BUCKETS
WITH (
  'changelog.mode' = 'upsert',
  'key.avro-registry.schema-context' = '.flink-dev',
  'value.avro-registry.schema-context' = '.flink-dev',
  'key.format' = 'avro-registry',
  'value.format' = 'avro-registry',
  'kafka.retention.time' = '0',
  'kafka.producer.compression.type' = 'snappy',
  'scan.bounded.mode' = 'unbounded',
  'scan.startup.mode' = 'earliest-offset',
  'value.fields-include' = 'all'
);