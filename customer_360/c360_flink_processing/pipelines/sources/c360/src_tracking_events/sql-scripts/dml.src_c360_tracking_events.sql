INSERT INTO src_c360_tracking_events
SELECT
    event_id,
    shipment_id,
    event_timestamp,
    event_type,
    location,
    description,
    carrier_status
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY `$rowtime` DESC
        ) AS row_num
    FROM tracking_events_raw
)
WHERE row_num = 1;
