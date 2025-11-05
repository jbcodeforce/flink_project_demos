INSERT INTO src_c360_app_usage
SELECT 
    usage_id,
    customer_id,
    session_date,
    session_start,
    session_duration_minutes,
    pages_viewed,
    actions_taken,
    device_type,
    app_version
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY usage_id 
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM app_usage_raw
)
WHERE row_num = 1