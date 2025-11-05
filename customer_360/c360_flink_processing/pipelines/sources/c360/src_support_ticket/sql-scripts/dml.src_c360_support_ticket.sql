INSERT INTO src_c360_support_ticket
SELECT 
    ticket_id,
    customer_id,
    created_date,
    resolved_date,
    category,
    priority,
    status,
    channel,
    satisfaction_score
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ticket_id 
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM support_ticket_raw
)
WHERE row_num = 1